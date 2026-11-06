from __future__ import annotations

GEOHASH_BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz"


def encode_geohash(latitude: float, longitude: float, precision: int = 5) -> str:
    if not -90 <= latitude <= 90 or not -180 <= longitude <= 180:
        raise ValueError("Coordinates must be within WGS84 ranges")
    if precision < 1:
        raise ValueError("Geohash precision must be positive")

    latitude_range = [-90.0, 90.0]
    longitude_range = [-180.0, 180.0]
    bits = (16, 8, 4, 2, 1)
    encoded: list[str] = []
    bit_index = 0
    character = 0
    longitude_turn = True

    while len(encoded) < precision:
        current_range = longitude_range if longitude_turn else latitude_range
        coordinate = longitude if longitude_turn else latitude
        midpoint = sum(current_range) / 2
        if coordinate >= midpoint:
            character |= bits[bit_index]
            current_range[0] = midpoint
        else:
            current_range[1] = midpoint
        longitude_turn = not longitude_turn

        if bit_index < 4:
            bit_index += 1
        else:
            encoded.append(GEOHASH_BASE32[character])
            bit_index = 0
            character = 0

    return "".join(encoded)


def annualized_closure_rate(closures: int, days_business: float) -> float | None:
    if closures < 0 or days_business < 0:
        raise ValueError("Closures and business exposure must be non-negative")
    if days_business == 0:
        return None
    return closures * 365 / days_business * 100
