"use strict";

const PHILADELPHIA = [39.9526, -75.1636];
const map = L.map("map", { doubleClickZoom: false }).setView(PHILADELPHIA, 12);
L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
  maxZoom: 19,
  attribution: "&copy; OpenStreetMap contributors",
}).addTo(map);

const form = document.querySelector("#business-search");
const searchButton = document.querySelector("#search-button");
const searchMessage = document.querySelector("#search-message");
const longitudeInput = document.querySelector("#longitude");
const latitudeInput = document.querySelector("#latitude");
const radiusInput = document.querySelector("#radius");
const radiusLabel = document.querySelector("#radius-label");
const categoryInput = document.querySelector("#category");
const limitInput = document.querySelector("#limit");
const businessList = document.querySelector("#business-list");
const resultsTitle = document.querySelector("#results-title");
const resultsCount = document.querySelector("#results-count");
const resultsMessage = document.querySelector("#results-message");
const markerLayer = L.layerGroup().addTo(map);
const radiusLayer = L.layerGroup().addTo(map);
const serviceList = document.querySelector("#service-list");
const healthTitle = document.querySelector("#health-title");
const healthIndicator = document.querySelector("#health-indicator");
const mapToast = document.querySelector("#map-toast");
const zoneFields = {
  businesses: document.querySelector("#metric-businesses"),
  closures: document.querySelector("#metric-closures"),
  rate: document.querySelector("#metric-rate"),
  sentiment: document.querySelector("#metric-sentiment"),
};
let toastTimer;

function formatNumber(value, digits = 0) {
  if (value === null || value === undefined || !Number.isFinite(Number(value))) return "—";
  return Number(value).toLocaleString("es-CO", {
    maximumFractionDigits: digits,
    minimumFractionDigits: digits,
  });
}

function setMessage(element, text, isError = false) {
  element.textContent = text;
  element.classList.toggle("error", isError);
}

function showToast(text) {
  mapToast.textContent = text;
  mapToast.classList.remove("hidden");
  window.clearTimeout(toastTimer);
  toastTimer = window.setTimeout(() => mapToast.classList.add("hidden"), 2400);
}

async function requestJson(url, options) {
  const response = await fetch(url, options);
  let payload;
  try {
    payload = await response.json();
  } catch {
    throw new Error(`Respuesta inválida del servidor (HTTP ${response.status}).`);
  }
  if (!response.ok) {
    throw new Error(payload.error || `La solicitud falló (HTTP ${response.status}).`);
  }
  return payload;
}

function updateRadiusCircle() {
  radiusLayer.clearLayers();
  const lat = Number(latitudeInput.value);
  const lon = Number(longitudeInput.value);
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return;
  L.circle([lat, lon], {
    radius: Number(radiusInput.value),
    color: "#e3923e",
    weight: 1.5,
    fillColor: "#f0b46b",
    fillOpacity: 0.1,
  }).addTo(radiusLayer);
  L.circleMarker([lat, lon], {
    radius: 5,
    color: "#fff",
    weight: 2,
    fillColor: "#e3923e",
    fillOpacity: 1,
  }).addTo(radiusLayer);
}

function sentimentLabel(score) {
  if (!Number.isFinite(Number(score))) return "Sin dato";
  if (Number(score) > 0.15) return "Positivo";
  if (Number(score) < -0.15) return "Negativo";
  return "Neutral";
}

function popupContent(business) {
  const content = document.createElement("div");
  const title = document.createElement("div");
  title.className = "popup-title";
  title.textContent = business.name || "Restaurante";
  content.append(title);
  const lines = [
    `⭐ ${formatNumber(business.stars, 1)} · ${business.is_open ? "Abierto" : "Cerrado"}`,
    `Categorías: ${(business.categories || []).slice(0, 3).join(", ") || "No disponibles"}`,
    `Sentimiento: ${sentimentLabel(business.sentiment?.mean_score)}`,
    `Reseñas: ${formatNumber(business.life?.review_count)}`,
  ];
  lines.forEach((text) => {
    const line = document.createElement("div");
    line.className = "popup-line";
    line.textContent = text;
    content.append(line);
  });
  return content;
}

function renderBusinesses(payload) {
  markerLayer.clearLayers();
  businessList.replaceChildren();
  resultsCount.textContent = formatNumber(payload.count);
  resultsTitle.textContent = "Restaurantes cercanos";
  setMessage(resultsMessage, payload.count
    ? "Selecciona un local de la lista o un marcador para ver sus detalles."
    : "No se encontraron restaurantes con esos filtros.");

  const bounds = [];
  payload.results.forEach((business) => {
    const coordinates = business.location?.coordinates;
    if (!Array.isArray(coordinates) || coordinates.length !== 2) return;
    const [longitude, latitude] = coordinates;
    if (!Number.isFinite(longitude) || !Number.isFinite(latitude)) return;
    const marker = L.circleMarker([latitude, longitude], {
      radius: 7,
      color: "#fff",
      weight: 2,
      fillColor: business.is_open ? "#087f78" : "#df715e",
      fillOpacity: 0.9,
    }).bindPopup(popupContent(business));
    marker.addTo(markerLayer);
    bounds.push([latitude, longitude]);

    const card = document.createElement("button");
    card.type = "button";
    card.className = "business-card";
    card.setAttribute("aria-label", `Ver ${business.name || "restaurante"} en el mapa`);
    const info = document.createElement("span");
    info.className = "business-info";
    const name = document.createElement("strong");
    name.textContent = business.name || "Restaurante sin nombre";
    const details = document.createElement("small");
    details.textContent = `${business.primary_category || "Restaurante"} · ${business.city || "Philadelphia"}${business.is_open ? "" : " · Cerrado"}`;
    info.append(name, details);
    const meta = document.createElement("span");
    meta.className = "business-meta";
    const stars = document.createElement("strong");
    stars.textContent = `★ ${formatNumber(business.stars, 1)}`;
    const sentiment = document.createElement("span");
    sentiment.textContent = sentimentLabel(business.sentiment?.mean_score);
    meta.append(stars, document.createElement("br"), sentiment);
    card.append(info, meta);
    card.addEventListener("click", () => {
      map.setView([latitude, longitude], Math.max(map.getZoom(), 15));
      marker.openPopup();
    });
    businessList.append(card);
  });
  if (bounds.length > 1) map.fitBounds(bounds, { padding: [40, 40], maxZoom: 14 });
  else if (bounds.length === 1) map.setView(bounds[0], 15);
  updateRadiusCircle();
}

function renderZoneSummary(summary) {
  zoneFields.businesses.textContent = formatNumber(summary.businesses);
  zoneFields.closures.textContent = formatNumber(summary.closures);
  zoneFields.rate.textContent = summary.closure_rate_pct === null
    ? "—"
    : `${formatNumber(summary.closure_rate_pct, 2)} %`;
  zoneFields.sentiment.textContent = summary.average_sentiment === null
    ? "—"
    : `${formatNumber(summary.average_sentiment, 2)} · ${sentimentLabel(summary.average_sentiment)}`;
  document.querySelector("#metric-note").textContent =
    `Exposición agregada: ${formatNumber(summary.days_business)} días-negocio · mínimo ${formatNumber(summary.min_days_business)}.`;
}

async function searchBusinesses(event) {
  if (event) event.preventDefault();
  const longitude = Number(longitudeInput.value);
  const latitude = Number(latitudeInput.value);
  if (!Number.isFinite(longitude) || longitude < -180 || longitude > 180
      || !Number.isFinite(latitude) || latitude < -90 || latitude > 90) {
    setMessage(searchMessage, "Ingresa coordenadas válidas.", true);
    return;
  }

  const radius = Number(radiusInput.value);
  const category = categoryInput.value.trim();
  const params = new URLSearchParams({
    lon: String(longitude),
    lat: String(latitude),
    radius_m: String(radius),
    limit: limitInput.value,
  });
  if (category) params.set("category", category);

  searchButton.disabled = true;
  searchButton.firstChild.textContent = "Buscando...";
  setMessage(searchMessage, "Consultando MongoDB y calculando el resumen...");
  map.setView([latitude, longitude], Math.min(map.getZoom(), 13));
  updateRadiusCircle();
  try {
    const [businesses, summary] = await Promise.all([
      requestJson(`/businesses/near?${params}`),
      requestJson(`/zones/summary?${new URLSearchParams({
        lon: String(longitude),
        lat: String(latitude),
        radius_m: String(radius),
        min_days_business: "365",
        ...(category ? { category } : {}),
      })}`),
    ]);
    renderBusinesses(businesses);
    renderZoneSummary(summary);
    setMessage(searchMessage, `${businesses.count} locales cargados desde Yelp.`);
  } catch (error) {
    setMessage(searchMessage, error.message, true);
    setMessage(resultsMessage, error.message, true);
    showToast(error.message);
  } finally {
    searchButton.disabled = false;
    searchButton.firstChild.textContent = "Buscar en el mapa ";
  }
}

async function refreshHealth() {
  try {
    const response = await fetch("/health");
    const health = await response.json();
    const services = health.services || {};
    serviceList.replaceChildren();
    Object.entries(services).forEach(([name, status]) => {
      const chip = document.createElement("span");
      chip.className = `service-chip${status === "ok" ? "" : " unavailable"}`;
      chip.textContent = `${name}: ${status}`;
      serviceList.append(chip);
    });
    const healthy = response.ok && health.status === "ok";
    healthIndicator.classList.toggle("online", healthy);
    healthIndicator.classList.toggle("offline", !healthy);
    healthTitle.textContent = healthy
      ? `Servicios disponibles · ${health.commit || "commit desconocido"}`
      : "Uno o más servicios no están disponibles";
  } catch (error) {
    healthIndicator.classList.remove("online");
    healthIndicator.classList.add("offline");
    healthTitle.textContent = "No se pudo consultar /health";
    serviceList.replaceChildren();
    const chip = document.createElement("span");
    chip.className = "service-chip unavailable";
    chip.textContent = error.message;
    serviceList.append(chip);
  }
}

async function predictSentiment(event) {
  event.preventDefault();
  const button = document.querySelector("#predict-button");
  const result = document.querySelector("#prediction-result");
  const text = document.querySelector("#review-text").value.trim();
  if (!text) return;
  button.disabled = true;
  button.textContent = "Analizando...";
  result.classList.remove("hidden", "error");
  result.textContent = "Consultando el modelo de sentimiento...";
  try {
    const prediction = await requestJson("/predict", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ text }),
    });
    result.replaceChildren();
    const label = document.createElement("strong");
    label.textContent = `Resultado: ${prediction.sentiment}`;
    result.append(label);
    const probabilities = document.createElement("div");
    probabilities.className = "probability-list";
    Object.entries(prediction.probabilities || {}).forEach(([sentiment, probability]) => {
      const item = document.createElement("span");
      item.className = "probability";
      item.textContent = `${sentiment}: ${formatNumber(probability * 100, 1)} %`;
      probabilities.append(item);
    });
    result.append(probabilities);
  } catch (error) {
    result.classList.add("error");
    result.textContent = error.message;
  } finally {
    button.disabled = false;
    button.textContent = "Analizar reseña";
  }
}

form.addEventListener("submit", searchBusinesses);
radiusInput.addEventListener("input", () => {
  const meters = Number(radiusInput.value);
  radiusLabel.textContent = meters >= 1000
    ? `${formatNumber(meters / 1000, meters % 1000 ? 1 : 0)} km`
    : `${formatNumber(meters)} m`;
  updateRadiusCircle();
});
document.querySelector("#predict-form").addEventListener("submit", predictSentiment);
document.querySelector("#refresh-health").addEventListener("click", refreshHealth);
map.on("click", (event) => {
  longitudeInput.value = event.latlng.lng.toFixed(5);
  latitudeInput.value = event.latlng.lat.toFixed(5);
  updateRadiusCircle();
  showToast("Centro actualizado. Pulsa «Buscar en el mapa».");
});

refreshHealth();
updateRadiusCircle();
searchBusinesses();
