import json
import random
from confluent_kafka import Producer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "fleet_catalog"

REGIONS = [
    "Ile-de-France",
    "Normandie",
    "Bretagne",
    "Grand Est",
    "Auvergne-Rhône-Alpes"
]

VEHICLE_TYPES = ["Van", "Truck", "Electric Van", "Heavy Carrier"]


def generate_catalog():
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})

    print(f"[*] Publishing 40 vehicles to compacted topic '{TOPIC}'...")

    for i in range(1, 41):
        vehicle_id = f"V{i:04d}"

        # Génération d'une plaque d'immatriculation française fictive (ex: AB-123-CD)
        letters1 = "".join(random.choices("ABCDEFGHIJKLMNOPQRSTUVWXYZ", k=2))
        numbers = f"{random.randint(100, 999):03d}"
        letters2 = "".join(random.choices("ABCDEFGHIJKLMNOPQRSTUVWXYZ", k=2))
        plate = f"{letters1}-{numbers}-{letters2}"

        vehicle_data = {
            "vehicle_id": vehicle_id,
            "plate": plate,
            "vehicle_type": random.choice(VEHICLE_TYPES),
            "home_region": REGIONS[(i - 1) % len(REGIONS)],  # Répartition équitable sur les 5 régions
            "max_payload_kg": random.choice([1000, 1200, 2500, 3500])
        }

        # Envoi dans Kafka avec vehicle_id comme clé (essentiel pour le nettoyage/compaction)
        producer.produce(
            TOPIC,
            key=vehicle_id,
            value=json.dumps(vehicle_data)
        )

    producer.flush()
    print("[SUCCESS] Fleet catalog successfully published!")


if __name__ == "__main__":
    generate_catalog()