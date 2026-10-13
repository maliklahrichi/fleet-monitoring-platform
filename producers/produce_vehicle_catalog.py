
import json
import random

from confluent_kafka import Producer
from faker import Faker

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "fleet_catalog"

REGIONS = ["Paris", "Lyon", "Marseille", "Lille", "Toulouse"]
VEHICLE_TYPES = ["van", "truck", "refrigerated_truck", "motorbike"]
PAYLOADS = [500, 1000, 1500, 3000]

SEED = 42  # graine fixe → mêmes données à chaque exécution


def build_catalog():
    """Construit les 40 enregistrements du catalogue."""
    rng = random.Random(SEED)
    fake = Faker("fr_FR")
    Faker.seed(SEED)

    catalog = []
    for i in range(1, 41):
        vehicle_id = f"V{i:04d}"           # V0001 ... V0040
        catalog.append({
            "vehicle_id": vehicle_id,
            "plate": fake.license_plate(),  # ex. "AB-123-CD"
            "vehicle_type": rng.choice(VEHICLE_TYPES),
            "home_region": REGIONS[(i - 1) % len(REGIONS)],  # 8 véhicules par région
            "max_payload_kg": rng.choice(PAYLOADS),
        })
    return catalog



def main():
    producer = Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "enable.idempotence": True,   # PID + numéros de séquence → pas de doublons
        "acks": "all",                # (imposé par l'idempotence, écrit pour être explicite)
    })

    for record in build_catalog():
        producer.produce(
            TOPIC,
            key=record["vehicle_id"],          # la clé : indispensable pour la compaction
            value=json.dumps(record),
        )
        producer.poll(0)  # traite les callbacks de livraison au fil de l'eau

    # Attend que tous les messages soient confirmés avant de quitter
    remaining = producer.flush(10)
    if remaining:
        print(f"{remaining} message(s) non confirmé(s) !")
    else:
        print("Catalogue publié : 40 véhicules.")


if __name__ == "__main__":
    main()