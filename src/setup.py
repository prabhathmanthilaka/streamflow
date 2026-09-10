from .common import create_topics, wait_for_service

if __name__ == "__main__":
    print("[SETUP] Waiting for Kafka and Schema Registry...")
    wait_for_service()
    create_topics()
    print("[SETUP] Ready.")
