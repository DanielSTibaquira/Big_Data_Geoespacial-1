"use strict";

const PORTO = [41.1579, -8.6291];
const map = L.map("map", { doubleClickZoom: false }).setView(PORTO, 13);
L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
  maxZoom: 19,
  attribution: "&copy; OpenStreetMap contributors",
}).addTo(map);

const connection = document.querySelector("#connection");
const connectionText = document.querySelector("#connection-text");
const message = document.querySelector("#message");
const resultCount = document.querySelector("#results-count");
const resultList = document.querySelector("#result-list");
const resultTag = document.querySelector("#result-tag");
const mapCaption = document.querySelector("#map-caption");
const mapToast = document.querySelector("#map-toast");
const longitudeInput = document.querySelector("#longitude");
const latitudeInput = document.querySelector("#latitude");
const radiusInput = document.querySelector("#radius");
const radiusValue = document.querySelector("#radius-value");

let mode = "near";
let pickingCenter = false;
let drawing = false;
let polygonPoints = [];
let polygonPreview = null;
let resultLayers = L.layerGroup().addTo(map);
let centerLayer = L.layerGroup().addTo(map);
let toastTimer = null;

function setMessage(text, isError = false) {
  message.textContent = text;
  message.classList.toggle("error", isError);
}

function showToast(text) {
  mapToast.textContent = text;
  mapToast.classList.remove("hidden");
  window.clearTimeout(toastTimer);
  toastTimer = window.setTimeout(() => mapToast.classList.add("hidden"), 2600);
}

function setLoading(button, loading, label) {
  button.disabled = loading;
  if (loading) {
    button.dataset.label = button.textContent.trim();
    button.textContent = "Consultando...";
  } else {
    button.innerHTML = label;
  }
}

async function requestJson(url, options) {
  const response = await fetch(url, options);
  let payload;
  try {
    payload = await response.json();
  } catch {
    throw new Error(`El servidor respondió HTTP ${response.status} sin JSON válido.`);
  }
  if (!response.ok) {
    throw new Error(payload.error || `La solicitud falló (HTTP ${response.status}).`);
  }
  return payload;
}

async function checkApi() {
  try {
    await requestJson("/health");
    connection.classList.add("online");
    connection.classList.remove("offline");
    connectionText.textContent = "API conectada · MongoDB disponible";
  } catch (error) {
    connection.classList.add("offline");
    connection.classList.remove("online");
    connectionText.textContent = "API o MongoDB no disponibles";
    setMessage(error.message, true);
  }
}

function clearResults() {
  resultLayers.clearLayers();
  centerLayer.clearLayers();
  resultList.replaceChildren();
}

function addPopupLine(parent, text, className = "popup-line") {
  const line = document.createElement("div");
  line.className = className;
  line.textContent = text;
  parent.append(line);
}

function tripPopup(trip) {
  const content = document.createElement("div");
  addPopupLine(content, `Viaje ${trip.trip_id || "sin ID"}`, "popup-title");
  addPopupLine(content, `Tipo: ${trip.call_type || "no disponible"}`);
  addPopupLine(content, `Taxi: ${trip.taxi_id ?? "no disponible"}`);
  addPopupLine(content, `Duración: ${trip.trip_duration_seconds ?? "—"} s`);
  return content;
}

function aggregatePopup(aggregate) {
  const content = document.createElement("div");
  addPopupLine(content, "Agregado espacial", "popup-title");
  addPopupLine(content, `Hora UTC: ${aggregate.hour_utc}`);
  addPopupLine(content, `Viajes: ${aggregate.trip_count}`);
  return content;
}

function renderTrips(payload, label) {
  clearResults();
  resultTag.textContent = "VIAJES · MONGODB";
  resultCount.textContent = `${payload.count} viajes encontrados`;
  setMessage(label);

  const points = [];
  payload.results.forEach((trip, index) => {
    const coordinates = trip.location?.coordinates;
    if (!Array.isArray(coordinates) || coordinates.length !== 2) return;
    const [longitude, latitude] = coordinates;
    const marker = L.circleMarker([latitude, longitude], {
      radius: 6,
      color: "#ffffff",
      weight: 2,
      fillColor: "#087f78",
      fillOpacity: 0.92,
    }).bindPopup(tripPopup(trip));
    marker.addTo(resultLayers);
    points.push([latitude, longitude]);

    const card = document.createElement("div");
    card.className = "result-card";
    const details = document.createElement("div");
    const title = document.createElement("strong");
    title.textContent = `Viaje ${trip.trip_id || `#${index + 1}`}`;
    const subtitle = document.createElement("small");
    subtitle.textContent = `${trip.call_type || "Tipo N/D"} · Taxi ${trip.taxi_id ?? "N/D"}`;
    details.append(title, subtitle);
    const value = document.createElement("span");
    value.className = "result-value";
    value.textContent = `${trip.trip_duration_seconds ?? "—"} s`;
    card.append(details, value);
    card.addEventListener("click", () => {
      map.setView([latitude, longitude], Math.max(map.getZoom(), 15));
      marker.openPopup();
    });
    resultList.append(card);
  });

  if (points.length === 1) map.setView(points[0], 15);
  else if (points.length > 1) map.fitBounds(points, { padding: [35, 35], maxZoom: 15 });
}

function renderAggregates(payload) {
  clearResults();
  resultTag.textContent = "SPARK · MONGODB";
  resultCount.textContent = `${payload.count} celdas activas`;
  setMessage("Cada círculo representa el centro de una celda de 0,01°; su tamaño refleja el conteo de viajes.");

  const points = [];
  const counts = payload.results.map((item) => Number(item.trip_count) || 0);
  const maxCount = Math.max(...counts, 1);
  payload.results.forEach((aggregate, index) => {
    const coordinates = aggregate.cell_center?.coordinates;
    if (!Array.isArray(coordinates) || coordinates.length !== 2) return;
    const [longitude, latitude] = coordinates;
    const tripCount = Number(aggregate.trip_count) || 0;
    const marker = L.circleMarker([latitude, longitude], {
      radius: 5 + (tripCount / maxCount) * 10,
      color: "#fff",
      weight: 1.5,
      fillColor: "#ef9f45",
      fillOpacity: 0.76,
    }).bindPopup(aggregatePopup(aggregate));
    marker.addTo(resultLayers);
    points.push([latitude, longitude]);

    const card = document.createElement("div");
    card.className = "result-card";
    const details = document.createElement("div");
    const title = document.createElement("strong");
    title.textContent = `Celda ${aggregate.grid_lon}, ${aggregate.grid_lat}`;
    const subtitle = document.createElement("small");
    subtitle.textContent = `Hora UTC ${aggregate.hour_utc} · Centro ${latitude.toFixed(3)}, ${longitude.toFixed(3)}`;
    details.append(title, subtitle);
    const value = document.createElement("span");
    value.className = "result-value";
    value.textContent = `${tripCount.toLocaleString()} viajes`;
    card.append(details, value);
    card.addEventListener("click", () => {
      map.setView([latitude, longitude], Math.max(map.getZoom(), 14));
      marker.openPopup();
    });
    resultList.append(card);
  });

  if (points.length === 1) map.setView(points[0], 14);
}

function validateCoordinates(longitude, latitude) {
  if (!Number.isFinite(longitude) || longitude < -180 || longitude > 180) {
    throw new Error("La longitud debe estar entre -180 y 180.");
  }
  if (!Number.isFinite(latitude) || latitude < -90 || latitude > 90) {
    throw new Error("La latitud debe estar entre -90 y 90.");
  }
}

function drawCenter(longitude, latitude, radius) {
  centerLayer.clearLayers();
  L.circle([latitude, longitude], {
    radius,
    color: "#e29439",
    weight: 1.5,
    fillColor: "#f2b460",
    fillOpacity: 0.12,
  }).addTo(centerLayer);
  L.circleMarker([latitude, longitude], {
    radius: 6,
    color: "#fff",
    weight: 2,
    fillColor: "#e29439",
    fillOpacity: 1,
  }).addTo(centerLayer);
}

async function searchNear() {
  const button = document.querySelector("#search-near");
  const longitude = Number(longitudeInput.value);
  const latitude = Number(latitudeInput.value);
  const radius = Number(radiusInput.value);
  try {
    validateCoordinates(longitude, latitude);
    setLoading(button, true);
    setMessage("Buscando viajes cercanos...");
    const query = new URLSearchParams({
      longitude: String(longitude),
      latitude: String(latitude),
      radius_m: String(radius),
      limit: document.querySelector("#near-limit").value,
    });
    const payload = await requestJson(`/api/v1/trips/near?${query}`);
    drawCenter(longitude, latitude, radius);
    renderTrips(payload, `Radio de ${radius.toLocaleString()} m desde el punto seleccionado.`);
  } catch (error) {
    setMessage(error.message, true);
    resultCount.textContent = "No se pudo completar la búsqueda";
  } finally {
    setLoading(button, false, 'Buscar viajes <span aria-hidden="true">→</span>');
  }
}

function setDrawingState(enabled) {
  drawing = enabled;
  document.querySelector("#start-drawing").classList.toggle("hidden", enabled);
  document.querySelector("#search-polygon").classList.toggle("hidden", !enabled);
  document.querySelector("#undo-point").disabled = !enabled || polygonPoints.length === 0;
  document.querySelector("#clear-polygon").disabled = polygonPoints.length === 0;
  map.getContainer().style.cursor = enabled ? "crosshair" : "";
  showToast(enabled ? "Haz clic en el mapa para añadir vértices." : "Dibujo de polígono finalizado.");
}

function updatePolygonPreview() {
  if (polygonPreview) polygonPreview.remove();
  const latLngs = polygonPoints.map((point) => [point[1], point[0]]);
  if (latLngs.length >= 2) {
    polygonPreview = L.polyline(latLngs, {
      color: "#087f78",
      weight: 3,
      dashArray: "7 6",
    }).addTo(map);
  }
  document.querySelector("#undo-point").disabled = !drawing || polygonPoints.length === 0;
  document.querySelector("#clear-polygon").disabled = polygonPoints.length === 0;
  document.querySelector("#search-polygon").disabled = polygonPoints.length < 3;
  setMessage(polygonPoints.length < 3
    ? `Marca ${3 - polygonPoints.length} punto(s) más para formar un polígono.`
    : `${polygonPoints.length} vértices listos. Cierra el área para buscar.`);
}

function clearPolygon() {
  polygonPoints = [];
  if (polygonPreview) {
    polygonPreview.remove();
    polygonPreview = null;
  }
  resultLayers.clearLayers();
  document.querySelector("#undo-point").disabled = true;
  document.querySelector("#clear-polygon").disabled = true;
  document.querySelector("#search-polygon").disabled = true;
  setMessage("Dibuja un área sobre el mapa para consultar viajes.");
}

async function searchPolygon() {
  if (polygonPoints.length < 3) return;
  const button = document.querySelector("#search-polygon");
  setDrawingState(false);
  const coordinates = [...polygonPoints, polygonPoints[0]];
  const polygon = { type: "Polygon", coordinates: [coordinates] };
  if (polygonPreview) polygonPreview.remove();
  polygonPreview = L.polygon(
    polygonPoints.map((point) => [point[1], point[0]]),
    { color: "#087f78", weight: 2, fillColor: "#13a394", fillOpacity: 0.13 },
  ).addTo(map);

  try {
    setLoading(button, true);
    setMessage("Consultando viajes dentro del polígono...");
    const payload = await requestJson("/api/v1/trips/within", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        polygon,
        limit: Number(document.querySelector("#polygon-limit").value),
      }),
    });
    renderTrips(payload, "Viajes cuyo origen está dentro del polígono seleccionado.");
    map.fitBounds(polygonPreview.getBounds(), { padding: [35, 35], maxZoom: 15 });
  } catch (error) {
    setMessage(error.message, true);
    resultCount.textContent = "No se pudo completar la búsqueda";
  } finally {
    setLoading(button, false, 'Cerrar y buscar viajes <span aria-hidden="true">→</span>');
  }
}

async function searchAggregates() {
  const button = document.querySelector("#search-aggregates");
  try {
    setLoading(button, true);
    setMessage("Cargando celdas agregadas de Spark...");
    const bounds = map.getBounds();
    const query = new URLSearchParams({
      min_lon: String(bounds.getWest()),
      min_lat: String(bounds.getSouth()),
      max_lon: String(bounds.getEast()),
      max_lat: String(bounds.getNorth()),
      limit: "500",
    });
    const payload = await requestJson(`/api/v1/aggregates/within?${query}`);
    renderAggregates(payload);
  } catch (error) {
    setMessage(error.message, true);
    resultCount.textContent = "No se pudieron cargar los agregados";
  } finally {
    setLoading(button, false, 'Consultar área visible <span aria-hidden="true">→</span>');
  }
}

document.querySelectorAll(".mode-button").forEach((button) => {
  button.addEventListener("click", () => {
    mode = button.dataset.mode;
    pickingCenter = false;
    if (drawing) setDrawingState(false);
    document.querySelectorAll(".mode-button").forEach((item) => {
      item.classList.toggle("active", item === button);
    });
    clearPolygon();
    document.querySelector("#near-panel").classList.toggle("hidden", mode !== "near");
    document.querySelector("#polygon-panel").classList.toggle("hidden", mode !== "polygon");
    document.querySelector("#aggregates-panel").classList.toggle("hidden", mode !== "aggregates");
    mapCaption.textContent = {
      near: "Vista de viajes cercanos",
      polygon: "Consulta por polígono",
      aggregates: "Agregados espaciales de Spark",
    }[mode];
    resultTag.textContent = mode === "aggregates" ? "SPARK · MONGODB" : "MONGODB";
    setMessage(mode === "polygon"
      ? "Dibuja un área sobre el mapa para consultar viajes."
      : mode === "aggregates"
        ? "Consulta los resúmenes Spark dentro del área visible."
        : "Elige una consulta para mostrar datos en el mapa.");
    resultCount.textContent = "Listo para explorar";
    clearResults();
  });
});

radiusInput.addEventListener("input", () => {
  radiusValue.textContent = `${Number(radiusInput.value).toLocaleString()} m`;
});
document.querySelector("#search-near").addEventListener("click", searchNear);
document.querySelector("#search-aggregates").addEventListener("click", searchAggregates);
document.querySelector("#start-drawing").addEventListener("click", () => {
  clearPolygon();
  setDrawingState(true);
});
document.querySelector("#undo-point").addEventListener("click", () => {
  if (!drawing || polygonPoints.length === 0) return;
  polygonPoints.pop();
  updatePolygonPreview();
});
document.querySelector("#clear-polygon").addEventListener("click", clearPolygon);
document.querySelector("#search-polygon").addEventListener("click", searchPolygon);
document.querySelector("#pick-center").addEventListener("click", (event) => {
  pickingCenter = true;
  event.currentTarget.classList.add("active");
  showToast("Haz clic en el mapa para elegir el centro del radio.");
});

map.on("click", (event) => {
  const longitude = event.latlng.lng;
  const latitude = event.latlng.lat;
  if (pickingCenter && mode === "near") {
    longitudeInput.value = longitude.toFixed(6);
    latitudeInput.value = latitude.toFixed(6);
    pickingCenter = false;
    document.querySelector("#pick-center").classList.remove("active");
    drawCenter(longitude, latitude, Number(radiusInput.value));
    showToast("Centro de búsqueda actualizado.");
    return;
  }
  if (drawing && mode === "polygon") {
    polygonPoints.push([longitude, latitude]);
    L.circleMarker([latitude, longitude], {
      radius: 4,
      color: "#fff",
      weight: 1.5,
      fillColor: "#087f78",
      fillOpacity: 1,
    }).addTo(resultLayers);
    updatePolygonPreview();
  }
});

checkApi();
