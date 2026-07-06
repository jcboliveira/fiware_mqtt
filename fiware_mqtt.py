#!/usr/bin/env python3

"""FIWARE to MQTT bridge.
Fetches sensor data from the FIWARE context broker and publishes it to an MQTT broker.
Optionally exposes Home Assistant MQTT discovery configuration for the sensors.
"""

import json
import time
import os
import threading
import requests
import paho.mqtt.client as mqtt
from datetime import datetime, timezone, timedelta
import argparse
import re
import unicodedata

# FIWARE API endpoint used to fetch entities.
FIWARE_URL = "https://broker.fiware.urbanplatform.portodigital.pt/v2/entities"

# MQTT configuration constants.
MQTT_PORT = 1883
DISCOVERY_PREFIX = "homeassistant"

published_discovery = set()
published_trackers = set()

mqtt_connected = False
mqtt_lock = threading.Lock()


# CLI argument parsing.
# The bridge supports filtering stations and enabling Home Assistant discovery.
def parse_args():
    parser = argparse.ArgumentParser(description="FIWARE → MQTT Bridge (CLI)")

    parser.add_argument(
        "--stations",
        type=str,
        help="Lista de estações permitidas (nomes separados por vírgulas)",
    )
    parser.add_argument(
        "--exclude",
        type=str,
        help="Lista de estações a excluir (nomes separados por vírgulas)",
    )

    parser.add_argument(
        "--mqtt-host", type=str, required=True, help="Endereço IP do broker MQTT"
    )
    parser.add_argument(
        "--mqtt-user", type=str, required=True, help="Username do broker MQTT"
    )
    parser.add_argument(
        "--mqtt-pass", type=str, required=True, help="Password do broker MQTT"
    )

    parser.add_argument(
        "--list-stations",
        action="store_true",
        help="Lista as estações disponíveis e termina",
    )

    parser.add_argument(
        "--homeassistant-discovery",
        action="store_true",
        help="Ativa MQTT Discovery para Home Assistant",
    )

    return parser.parse_args()


# Decide whether a station is eligible based on include/exclude CLI options.
def station_allowed(local_name, args):
    name = local_name.lower()

    if args.stations:
        allowed = [s.strip().lower() for s in args.stations.split(",")]
        return name in allowed

    if args.exclude:
        blocked = [s.strip().lower() for s in args.exclude.split(",")]
        return name not in blocked

    return True


# MQTT callback functions. These update the shared connection state
# and provide useful logging for broker connectivity.
def on_connect(client, userdata, flags, rc):
    global mqtt_connected
    mqtt_connected = rc == 0
    if rc == 0:
        print("INFO: Connected to MQTT broker.")
    else:
        print(f"ERROR: Failed to connect to broker (rc={rc}).")


# Called when the MQTT connection is lost.
# It resets the shared connection flag so the reconnect thread can retry.
def on_disconnect(client, userdata, rc):
    global mqtt_connected
    mqtt_connected = False
    print("WARNING: Disconnected from MQTT broker.")


# Background task that retries MQTT connection when disconnected.
# This keeps the bridge online if the broker temporarily becomes unavailable.
def mqtt_reconnect_loop(client, args):
    global mqtt_connected
    delay = 1
    while True:
        if not mqtt_connected:
            with mqtt_lock:
                try:
                    print(f"WARNING: Attempting MQTT reconnect (delay={delay}s)...")
                    client.connect(args.mqtt_host, MQTT_PORT, 60)
                    delay = 1
                except Exception as e:
                    print(f"ERROR: Reconnect failed: {e}")
                    time.sleep(delay)
                    delay = min(delay * 2, 60)
        time.sleep(1)


# Initialize MQTT client, connect to the broker, and start the network loop.
# A reconnect thread is launched so the bridge can recover automatically.
def mqtt_init(args):
    client = mqtt.Client()
    client.username_pw_set(args.mqtt_user, args.mqtt_pass)
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect

    while True:
        try:
            client.connect(args.mqtt_host, MQTT_PORT, 60)
            break
        except Exception as e:
            print(f"ERROR: Initial MQTT connect failed: {e}. Retrying in 3s...")
            time.sleep(3)

    client.loop_start()
    threading.Thread(
        target=mqtt_reconnect_loop, args=(client, args), daemon=True
    ).start()
    return client


# Extract the human-readable station name from a FIWARE entity payload.
def get_station_name(entity):
    return entity.get("name", {}).get("value", "Unknown").strip()


# Calculate AQI for a pollutant concentration using defined breakpoints.
# Returns None when the concentration is outside the available ranges.
def calc_aqi(value, breakpoints):
    for bp in breakpoints:
        c_low, c_high, aqi_low, aqi_high = bp
        if c_low <= value <= c_high:
            return round(
                ((aqi_high - aqi_low) / (c_high - c_low)) * (value - c_low) + aqi_low
            )
    return None


# Compute the maximum AQI across supported pollutant measurements.
# This matches the common AQI definition of using the worst pollutant.
def compute_aqi(entity):
    aqi_values = []

    if "pm25" in entity:
        aqi = calc_aqi(
            entity["pm25"]["value"],
            [
                (0.0, 12.0, 0, 50),
                (12.1, 35.4, 51, 100),
                (35.5, 55.4, 101, 150),
                (55.5, 150.4, 151, 200),
            ],
        )
        if aqi:
            aqi_values.append(aqi)

    if "pm10" in entity:
        aqi = calc_aqi(
            entity["pm10"]["value"],
            [
                (0, 54, 0, 50),
                (55, 154, 51, 100),
                (155, 254, 101, 150),
            ],
        )
        if aqi:
            aqi_values.append(aqi)

    if "o3" in entity:
        aqi = calc_aqi(
            entity["o3"]["value"],
            [
                (0, 100, 0, 100),
                (101, 160, 101, 150),
            ],
        )
        if aqi:
            aqi_values.append(aqi)

    if "no2" in entity:
        aqi = calc_aqi(
            entity["no2"]["value"],
            [
                (0, 100, 0, 100),
                (101, 200, 101, 150),
            ],
        )
        if aqi:
            aqi_values.append(aqi)

    return max(aqi_values) if aqi_values else None


# Determine which pollutant contributes the highest AQI value.
# Returns a pollutant key such as pm25 or no2, or 'unknown' when none are available.
def compute_main_pollutant(entity):
    pollutants = {}

    if "pm25" in entity:
        pollutants["pm25"] = calc_aqi(
            entity["pm25"]["value"],
            [
                (0.0, 12.0, 0, 50),
                (12.1, 35.4, 51, 100),
                (35.5, 55.4, 101, 150),
                (55.5, 150.4, 151, 200),
            ],
        )

    if "pm10" in entity:
        pollutants["pm10"] = calc_aqi(
            entity["pm10"]["value"],
            [
                (0, 54, 0, 50),
                (55, 154, 51, 100),
                (155, 254, 101, 150),
            ],
        )

    if "o3" in entity:
        pollutants["o3"] = calc_aqi(
            entity["o3"]["value"],
            [
                (0, 100, 0, 100),
                (101, 160, 101, 150),
            ],
        )

    if "no2" in entity:
        pollutants["no2"] = calc_aqi(
            entity["no2"]["value"],
            [
                (0, 100, 0, 100),
                (101, 200, 101, 150),
            ],
        )

    pollutants = {k: v for k, v in pollutants.items() if v is not None}

    return max(pollutants, key=pollutants.get) if pollutants else "unknown"


# Fetch entities of the requested FIWARE type, retrying on failure.
# Uses exponential backoff to tolerate transient network or API issues.
def fetch_fiware(type_name):
    delay = 1
    while True:
        try:
            r = requests.get(FIWARE_URL, params={"type": type_name}, timeout=10)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            print(f"ERROR FIWARE ({type_name}): {e}. Retrying in {delay}s...")
            time.sleep(delay)
            delay = min(delay * 2, 60)


# Publish raw sensor values from a FIWARE entity into MQTT topics.
# The values are published under a sensor-specific base topic, with optional AQI fields.
def publish_values(
    client,
    entity_id,
    entity,
    local_name,
    sensor_base,
    sensor_fields,
    extra_fields,
    include_aqi=False,
):
    for field in sensor_fields:
        if field in entity:
            value = entity[field]["value"]

            if field == "relativeHumidity":
                try:
                    if value <= 1:
                        value = round(value * 100, 1)
                except:
                    pass

            if field == "windSpeed":
                try:
                    value = round(value * 3.6, 1)
                except:
                    pass

            client.publish(f"{sensor_base}/{entity_id}/{field}", value, retain=True)

    client.publish(f"{sensor_base}/{entity_id}/local", local_name, retain=True)

    if "dateObserved" in entity:
        client.publish(
            f"{sensor_base}/{entity_id}/dateObserved",
            entity["dateObserved"]["value"],
            retain=True,
        )

    now = datetime.now(timezone.utc).isoformat()
    client.publish(f"{sensor_base}/{entity_id}/last_mqtt_update", now, retain=True)

    if include_aqi:
        aqi = compute_aqi(entity)
        if aqi is not None:
            client.publish(f"{sensor_base}/{entity_id}/aqi", aqi, retain=True)

        main = compute_main_pollutant(entity)
        client.publish(f"{sensor_base}/{entity_id}/main_pollutant", main, retain=True)


# Parse FIWARE datetime strings into timezone-aware Python datetimes.
# Handles both ISO 8601 forms with and without trailing Z.
def parse_fiware_datetime(dt):
    if dt.endswith("Z"):
        dt = dt[:-1] + "+00:00"

    dt = dt.replace(".00+", "+").replace(".000+", "+").replace(".0+", "+")

    try:
        return datetime.fromisoformat(dt)
    except:
        from dateutil import parser

        return parser.parse(dt)


# Log detailed observation metadata for debugging or data quality checks.
# This helper prints a compact message with entity id, station name, observation
# timestamp and computed age.
def log_observation_check(entity, local_name, entity_id, date_obs_str, date_obs, age):
    print(
        f"OBS_CHECK | "
        f"id={entity.get('id')} | "
        f"name={local_name} | "
        f"entity_id={entity_id} | "
        f"dateObserved={date_obs_str} | "
        f"parsed={date_obs} | "
        f"age={age}"
    )


# Print a deduplicated list of station names available from FIWARE.
# This helper is used when the user passes --list-stations.
def list_stations():
    print("Fetching list of FIWARE stations...")

    # Fetch both air quality and weather station entities.
    aq = fetch_fiware("AirQualityObserved")
    wt = fetch_fiware("WeatherObserved")

    # Collect station names from both entity types.
    names = []

    for entity in aq + wt:
        local_name = get_station_name(entity)

        # Ignore any entity without a valid display name.
        if not local_name or local_name == "Unknown":
            print(f"WARNING: Entity missing name: " f"{entity['id']}")
            continue

        names.append(local_name)

    # Remove duplicates and sort for a clean output.
    names = sorted(set(names))

    print("\nAvailable stations:\n")
    for name in names:
        print(f" - {name}")

    print("\nUse these names with --stations or --exclude.\n")


# Normalize station names to a safe MQTT-friendly identifier.
# This removes accents and non-alphanumeric characters.
def normalize_station_name(name):
    name = unicodedata.normalize("NFD", name)
    name = "".join(c for c in name if unicodedata.category(c) != "Mn")
    name = name.lower()
    name = re.sub(r"[^a-z0-9]+", "_", name)
    return name.strip("_")


# Publish Home Assistant MQTT discovery configuration for a sensor.
# This allows HA to auto-discover the sensor and its MQTT topic.
def publish_discovery(
    client,
    unique_id,
    station_name,
    sensor_name,
    state_topic,
    unit=None,
    device_class=None,
    state_class="measurement",
):

    config_topic = f"homeassistant/sensor/" f"{unique_id}/config"

    payload = {
        "name": sensor_name,
        "unique_id": unique_id,
        "state_topic": state_topic,
        "device": {
            "identifiers": [f"fiware_{normalize_station_name(station_name)}"],
            "name": station_name,
            "manufacturer": "Porto Digital",
            "model": "FIWARE",
        },
    }

    if unit:
        payload["unit_of_measurement"] = unit

    if device_class:
        payload["device_class"] = device_class

    if state_class:
        payload["state_class"] = state_class

    # Avoid publishing the same discovery config repeatedly.
    if unique_id in published_discovery:
        return

    print(f"DISCOVERY {unique_id}")

    published_discovery.add(unique_id)

    client.publish(config_topic, json.dumps(payload), retain=True)


# Create discovery payloads for weather-related sensor fields.
def publish_weather_discovery(client, entity_id, station_name):

    base = f"fiware/weather/{entity_id}"

    publish_discovery(
        client,
        f"{entity_id}_temperature",
        station_name,
        "Temperatura",
        f"{base}/temperature",
        unit="°C",
        device_class="temperature",
    )

    publish_discovery(
        client,
        f"{entity_id}_humidity",
        station_name,
        "Humidade",
        f"{base}/relativeHumidity",
        unit="%",
        device_class="humidity",
    )

    publish_discovery(
        client,
        f"{entity_id}_windspeed",
        station_name,
        "Velocidade do vento",
        f"{base}/windSpeed",
        unit="km/h",
    )

    publish_discovery(
        client,
        f"{entity_id}_precipitation",
        station_name,
        "Precipitação",
        f"{base}/precipitation",
        unit="mm",
    )

    publish_discovery(
        client, f"{entity_id}_uv", station_name, "Índice UV", f"{base}/uVIndexMax"
    )

    publish_discovery(
        client,
        f"{entity_id}_latitude",
        station_name,
        "Latitude",
        f"{base}/latitude",
        state_class=None,
    )

    publish_discovery(
        client,
        f"{entity_id}_longitude",
        station_name,
        "Longitude",
        f"{base}/longitude",
        state_class=None,
    )


# Create discovery payloads for air quality-related sensor fields.
def publish_airquality_discovery(client, entity_id, station_name):

    base = f"fiware/airquality/{entity_id}"

    publish_discovery(
        client, f"{entity_id}_pm25", station_name, "PM2.5", f"{base}/pm25", unit="µg/m³"
    )

    publish_discovery(
        client, f"{entity_id}_pm10", station_name, "PM10", f"{base}/pm10", unit="µg/m³"
    )

    publish_discovery(
        client, f"{entity_id}_no2", station_name, "NO₂", f"{base}/no2", unit="µg/m³"
    )

    publish_discovery(
        client, f"{entity_id}_o3", station_name, "O₃", f"{base}/o3", unit="µg/m³"
    )

    publish_discovery(
        client, f"{entity_id}_co", station_name, "CO", f"{base}/co", unit="µg/m³"
    )

    publish_discovery(client, f"{entity_id}_aqi", station_name, "AQI", f"{base}/aqi")

    publish_discovery(
        client,
        f"{entity_id}_main_pollutant",
        station_name,
        "Poluente Principal",
        f"{base}/main_pollutant",
        state_class=None,
    )

    publish_discovery(
        client,
        f"{entity_id}_latitude",
        station_name,
        "Latitude",
        f"{base}/latitude",
        state_class=None,
    )

    publish_discovery(
        client,
        f"{entity_id}_longitude",
        station_name,
        "Longitude",
        f"{base}/longitude",
        state_class=None,
    )


# Publish a device tracker for Home Assistant so the station location
# is available as a tracked entity in HA.
def publish_device_tracker_discovery(client, entity_id, station_name, lat, lon):

    config_topic = f"homeassistant/device_tracker/" f"{entity_id}/config"

    payload = {
        "name": station_name,
        "unique_id": f"{entity_id}_tracker",
        "state_topic": f"fiware/location/{entity_id}/state",
        "json_attributes_topic": f"fiware/location/{entity_id}/attributes",
        "source_type": "gps",
        "device": {
            "identifiers": [f"fiware_{entity_id}"],
            "name": station_name,
            "manufacturer": "Porto Digital",
            "model": "FIWARE",
        },
    }

    if entity_id in published_trackers:
        return

    published_trackers.add(entity_id)

    client.publish(config_topic, json.dumps(payload), retain=True)

    client.publish(f"fiware/location/{entity_id}/state", "home", retain=True)

    client.publish(
        f"fiware/location/{entity_id}/attributes",
        json.dumps({"latitude": lat, "longitude": lon}),
        retain=True,
    )


# Remove retained discovery topics for a weather entity when it becomes stale.
def remove_weather_discovery(client, entity_id):

    sensors = [
        f"{entity_id}_temperature",
        f"{entity_id}_humidity",
        f"{entity_id}_windspeed",
        f"{entity_id}_precipitation",
        f"{entity_id}_uv",
        f"{entity_id}_latitude",
        f"{entity_id}_longitude",
    ]

    for sensor_id in sensors:

        client.publish(f"homeassistant/sensor/{sensor_id}/config", "", retain=True)

        published_discovery.discard(sensor_id)

    print(f"INFO: Weather discovery removed " f"for '{entity_id}'")


# Remove retained discovery topics for an air quality entity when it becomes stale.
def remove_airquality_discovery(client, entity_id):

    sensors = [
        f"{entity_id}_pm25",
        f"{entity_id}_pm10",
        f"{entity_id}_no2",
        f"{entity_id}_o3",
        f"{entity_id}_co",
        f"{entity_id}_aqi",
        f"{entity_id}_main_pollutant",
    ]

    for sensor_id in sensors:

        client.publish(f"homeassistant/sensor/{sensor_id}/config", "", retain=True)

        published_discovery.discard(sensor_id)

    print(f"INFO: Air quality discovery removed " f"for '{entity_id}'")


# Main loop for fetching air quality entities, publishing values, and optionally discovery.
def loop_airquality(client, args):
    SENSOR_FIELDS_AQ = {
        "co": {},
        "no2": {},
        "o3": {},
        "pm1": {},
        "pm10": {},
        "pm25": {},
        "temperature": {},
    }

    EXTRA_FIELDS_AQ = {
        "local": {},
        "dateObserved": {},
        "last_mqtt_update": {},
        "aqi": {},
        "main_pollutant": {},
        "latitude": {},
        "longitude": {},
    }

    SENSOR_BASE_AQ = "fiware/airquality"

    while True:
        # Retrieve all AirQualityObserved entities from FIWARE.
        data = fetch_fiware("AirQualityObserved")
        print(f"INFO: {len(data)} AirQuality entities.")

        # Track duplicate names to generate unique entity IDs.
        name_counts = {}

        for entity in data:
            lon, lat = entity["location"]["value"]["coordinates"]
            local_name = get_station_name(entity)

            # Skip entities without a usable station name.
            if not local_name or local_name == "Unknown":
                print(f"WARNING: Entity missing name: " f"{entity['id']}")
                continue

            # Apply include/exclude filtering from CLI arguments.
            if not station_allowed(local_name, args):
                print(f"WARNING: Station '{local_name}' filtered.")
                continue

            # Normalize the station name for safe MQTT topic use.
            safe_local = normalize_station_name(local_name)
            count = name_counts.get(safe_local, 0) + 1
            name_counts[safe_local] = count

            entity_id = f"{safe_local}_{count}" if count > 1 else safe_local

            # Validate observation timestamp and ignore stale measurements.
            date_obs_str = entity.get("dateObserved", {}).get("value")
            if date_obs_str:
                try:
                    date_obs = parse_fiware_datetime(date_obs_str)
                    age = datetime.now(timezone.utc) - date_obs

                    if age > timedelta(days=1):
                        print(
                            f"WARNING: {entity_id} ignored air quality (stale observation)."
                        )

                        remove_airquality_discovery(client, entity_id)
                        continue

                except Exception as e:
                    print(f"ERROR: Invalid date in AirQuality: {e}")

            # Log station mapping from FIWARE entity to local MQTT ID.
            print(f"INFO: Station '{local_name}' → ID '{entity_id}'")

            # Publish Home Assistant discovery if requested.
            if args.homeassistant_discovery:
                publish_airquality_discovery(client, entity_id, local_name)

            # Publish full sensor payloads for this air quality entity.
            publish_values(
                client,
                entity_id,
                entity,
                local_name,
                SENSOR_BASE_AQ,
                SENSOR_FIELDS_AQ,
                EXTRA_FIELDS_AQ,
                include_aqi=True,
            )

            # Publish location fields separately for use in MQTT topics.
            client.publish(f"{SENSOR_BASE_AQ}/{entity_id}/latitude", lat, retain=True)
            client.publish(f"{SENSOR_BASE_AQ}/{entity_id}/longitude", lon, retain=True)

            # Publish a Home Assistant device tracker for the station.
            if args.homeassistant_discovery:
                publish_device_tracker_discovery(
                    client, entity_id, local_name, lat, lon
                )

        # Wait before polling FIWARE again.
        time.sleep(60)


# Main loop for fetching weather entities, publishing values, and optionally discovery.
def loop_weather(client, args):
    SENSOR_FIELDS_W = {
        "precipitation": {},
        "temperature": {},
        "windSpeed": {},
        "relativeHumidity": {},
        "uVIndexMax": {},
        "uv_index": {},
    }

    EXTRA_FIELDS_W = {
        "local": {},
        "dateObserved": {},
        "last_mqtt_update": {},
        "latitude": {},
        "longitude": {},
    }

    SENSOR_BASE_W = "fiware/weather"

    while True:
        # Retrieve all WeatherObserved entities from FIWARE.
        data = fetch_fiware("WeatherObserved")
        print(f"INFO: {len(data)} Weather entities.")

        # Track duplicate station names to create unique MQTT IDs.
        name_counts = {}

        for entity in data:
            lon, lat = entity["location"]["value"]["coordinates"]
            local_name = get_station_name(entity)

            # Skip weather entities without a usable station name.
            if not local_name or local_name == "Unknown":
                print(f"WARNING: Entity missing name ignored: " f"{entity['id']}")
                continue

            # Apply include/exclude filters from the CLI options.
            if not station_allowed(local_name, args):
                print(f"WARNING: Station '{local_name}' filtered.")
                continue

            safe_local = normalize_station_name(local_name)
            count = name_counts.get(safe_local, 0) + 1
            name_counts[safe_local] = count

            entity_id = f"{safe_local}_{count}" if count > 1 else safe_local

            date_obs_str = entity.get("dateObserved", {}).get("value")
            if date_obs_str:
                try:
                    date_obs = parse_fiware_datetime(date_obs_str)
                    age = datetime.now(timezone.utc) - date_obs

                    # Ignore stale weather observations older than one day.
                    if age > timedelta(days=1):
                        print(
                            f"WARNING: {entity_id} ignored in weather (stale observation)."
                        )
                        remove_weather_discovery(client, entity_id)
                        continue

                except Exception as e:
                    print(f"ERROR: Invalid date in Weather: {e}")

            # Log station mapping and current entity ID.
            print(f"INFO: Station '{local_name}' → ID '{entity_id}'")

            # Publish Home Assistant discovery if requested.
            if args.homeassistant_discovery:
                publish_weather_discovery(client, entity_id, local_name)

            # Publish sensor values for this weather entity.
            publish_values(
                client,
                entity_id,
                entity,
                local_name,
                SENSOR_BASE_W,
                SENSOR_FIELDS_W,
                EXTRA_FIELDS_W,
            )

            # Publish location coordinates separately for this weather station.
            client.publish(f"{SENSOR_BASE_W}/{entity_id}/latitude", lat, retain=True)
            client.publish(f"{SENSOR_BASE_W}/{entity_id}/longitude", lon, retain=True)

            # Publish a device tracker for Home Assistant if discovery is active.
            if args.homeassistant_discovery:
                publish_device_tracker_discovery(
                    client, entity_id, local_name, lat, lon
                )

        # Pause between polling cycles to reduce FIWARE load.
        time.sleep(60)


if __name__ == "__main__":
    # Parse CLI arguments before starting the bridge.
    args = parse_args()

    # If the user only wants station names, display them and exit.
    if args.list_stations:
        list_stations()
        exit(0)

    # Initialize MQTT connection and start the reconnect thread.
    client = mqtt_init(args)

    # Start the air quality and weather polling loops in background threads.
    threading.Thread(target=loop_airquality, args=(client, args), daemon=True).start()
    threading.Thread(target=loop_weather, args=(client, args), daemon=True).start()

    # Keep the main thread alive while background threads run.
    while True:
        time.sleep(10)
