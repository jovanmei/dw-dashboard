"""
Windows-specific Kafka setup and startup script.

This script helps set up and start Kafka on Windows systems using either
Docker (recommended) or native Windows installation.
"""

import subprocess
import sys
import time
import os
import json
from pathlib import Path


def check_docker():
    """Check if Docker is available and running."""
    try:
        result = subprocess.run(["docker", "--version"], capture_output=True, text=True)
        if result.returncode == 0:
            print(f"✅ Docker found: {result.stdout.strip()}")
            
            # Check if Docker daemon is running
            result = subprocess.run(["docker", "info"], capture_output=True, text=True)
            if result.returncode == 0:
                print("✅ Docker daemon is running")
                return True
            else:
                print("❌ Docker daemon is not running")
                print("   Please start Docker Desktop and wait for it to be ready")
                return False
        else:
            print("❌ Docker not found")
            return False
    except FileNotFoundError:
        print("❌ Docker not installed")
        print("   Download from: https://www.docker.com/products/docker-desktop")
        return False


def create_docker_compose():
    """Create docker-compose.yml for Kafka."""
    docker_compose_content = """services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.4.0
    hostname: zookeeper
    container_name: zookeeper
    ports:
      - "2181:2181"
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    healthcheck:
      test: ["CMD", "bash", "-c", "echo 'ruok' | nc localhost 2181"]
      interval: 10s
      timeout: 5s
      retries: 5

  kafka:
    image: confluentinc/cp-kafka:7.4.0
    hostname: kafka
    container_name: kafka
    depends_on:
      zookeeper:
        condition: service_healthy
    ports:
      - "9092:9092"
      - "9101:9101"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: 'zookeeper:2181'
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_JMX_PORT: 9101
      KAFKA_JMX_HOSTNAME: localhost
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: 'true'
    healthcheck:
      test: ["CMD", "bash", "-c", "kafka-broker-api-versions --bootstrap-server localhost:9092"]
      interval: 10s
      timeout: 5s
      retries: 5

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    container_name: kafka-ui
    depends_on:
      kafka:
        condition: service_healthy
    ports:
      - "8080:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:29092
      KAFKA_CLUSTERS_0_ZOOKEEPER: zookeeper:2181
"""
    
    with open("docker-compose.yml", "w") as f:
        f.write(docker_compose_content)
    
    print("✅ Created docker-compose.yml")


def start_kafka_docker():
    """Start Kafka using Docker Compose."""
    print("🚀 Starting Kafka with Docker...")
    
    # Create docker-compose.yml if it doesn't exist
    if not os.path.exists("docker-compose.yml"):
        create_docker_compose()
    
    try:
        # Start services
        print("   Starting Zookeeper and Kafka...")
        result = subprocess.run(
            ["docker-compose", "up", "-d"],
            capture_output=True,
            text=True
        )
        
        if result.returncode != 0:
            print(f"❌ Failed to start Docker services: {result.stderr}")
            return False
        
        print("✅ Docker services started")
        
        # Wait for services to be healthy
        print("   Waiting for services to be ready...")
        max_wait = 120  # 2 minutes
        wait_time = 0
        
        while wait_time < max_wait:
            # Check if Kafka is ready
            health_result = subprocess.run(
                ["docker-compose", "ps", "--format", "json"],
                capture_output=True,
                text=True
            )
            
            if health_result.returncode == 0:
                try:
                    services = [json.loads(line) for line in health_result.stdout.strip().split('\n') if line]
                    kafka_healthy = any(
                        service.get('Name') == 'kafka' and 'healthy' in service.get('Health', '')
                        for service in services
                    )
                    
                    if kafka_healthy:
                        print("✅ Kafka is ready!")
                        break
                except:
                    pass
            
            time.sleep(5)
            wait_time += 5
            print(f"   Waiting... ({wait_time}s/{max_wait}s)")
        
        if wait_time >= max_wait:
            print("⚠️ Kafka may not be fully ready, but continuing...")
        
        print("\n🎉 Kafka Services Started Successfully!")
        print("   Kafka: localhost:9092")
        print("   Kafka UI: http://localhost:8080")
        print("   Zookeeper: localhost:2181")
        
        return True
        
    except Exception as e:
        print(f"❌ Error starting Kafka: {e}")
        return False


def stop_kafka_docker():
    """Stop Kafka Docker services."""
    print("🛑 Stopping Kafka services...")
    
    try:
        result = subprocess.run(
            ["docker-compose", "down"],
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            print("✅ Kafka services stopped")
        else:
            print(f"⚠️ Error stopping services: {result.stderr}")
        
    except Exception as e:
        print(f"❌ Error stopping Kafka: {e}")


def check_kafka_status():
    """Check if Kafka is running."""
    print("🔍 Checking Kafka status...")
    
    try:
        # Check Docker containers
        result = subprocess.run(
            ["docker", "ps", "--format", "table {{.Names}}\t{{.Status}}", "--filter", "name=kafka"],
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0 and "kafka" in result.stdout:
            print("✅ Kafka container is running")
            
            # Test connectivity
            try:
                from kafka import KafkaProducer
                producer = KafkaProducer(
                    bootstrap_servers=['localhost:9092'],
                    request_timeout_ms=5000
                )
                producer.close()
                print("✅ Kafka is accessible on localhost:9092")
                return True
            except Exception as e:
                print(f"⚠️ Kafka container running but not accessible: {e}")
                return False
        else:
            print("❌ Kafka container not running")
            return False
            
    except Exception as e:
        print(f"❌ Error checking Kafka status: {e}")
        return False


def download_kafka_windows():
    """Provide instructions for downloading Kafka on Windows."""
    print("📥 Kafka Windows Installation Guide:")
    print("=" * 50)
    print("1. Download Kafka:")
    print("   https://downloads.apache.org/kafka/3.6.0/kafka_2.13-3.6.0.tgz")
    print()
    print("2. Extract to C:\\kafka_2.13-3.6.0\\")
    print()
    print("3. Start services in separate Command Prompts:")
    print("   # Terminal 1 (Zookeeper):")
    print("   cd C:\\kafka_2.13-3.6.0")
    print("   bin\\windows\\zookeeper-server-start.bat config\\zookeeper.properties")
    print()
    print("   # Terminal 2 (Kafka):")
    print("   cd C:\\kafka_2.13-3.6.0")
    print("   bin\\windows\\kafka-server-start.bat config\\server.properties")
    print()
    print("4. Test with:")
    print("   python setup_kafka.py")


def main():
    """Main setup function."""
    print("🚀 Kafka Setup for Windows")
    print("=" * 40)
    
    while True:
        print("\nOptions:")
        print("  1. Start Kafka with Docker (Recommended)")
        print("  2. Stop Kafka Docker services")
        print("  3. Check Kafka status")
        print("  4. Native Windows installation guide")
        print("  5. Test Kafka connectivity")
        print("  6. Fix Docker issues")
        print("  7. Use Simple Kafka (No Docker)")
        print("  8. Exit")
        
        choice = input("\nEnter choice (1-8): ").strip()
        
        if choice == "1":
            if check_docker():
                if start_kafka_docker():
                    print("\n💡 Next steps:")
                    print("   1. Run: python setup_kafka.py")
                    print("   2. Run: python run_kafka_pipeline.py --with-dashboard")
                else:
                    print("\n❌ Failed to start Kafka")
            else:
                print("\n❌ Docker not available")
                print("   Install Docker Desktop: https://www.docker.com/products/docker-desktop")
                
        elif choice == "2":
            stop_kafka_docker()
            
        elif choice == "3":
            check_kafka_status()
            
        elif choice == "4":
            download_kafka_windows()
            
        elif choice == "5":
            # Test connectivity
            try:
                from kafka import KafkaProducer
                producer = KafkaProducer(
                    bootstrap_servers=['localhost:9092'],
                    request_timeout_ms=5000
                )
                producer.close()
                print("✅ Kafka connectivity test passed")
            except ImportError:
                print("❌ kafka-python not installed. Run: pip install kafka-python")
            except Exception as e:
                print(f"❌ Kafka connectivity test failed: {e}")
                
        elif choice == "6":
            # Fix Docker issues
            print("\n🔧 Docker Troubleshooting Guide:")
            print("=" * 50)
            print("1. **Restart Docker Desktop**:")
            print("   - Right-click Docker Desktop icon in system tray")
            print("   - Select 'Restart Docker Desktop'")
            print("   - Wait 2-3 minutes for full startup")
            print()
            print("2. **Check Docker Desktop Status**:")
            print("   - Look for Docker whale icon in system tray")
            print("   - Icon should be steady (not animated)")
            print("   - Click icon to see status")
            print()
            print("3. **Enable Required Windows Features**:")
            print("   - Open 'Turn Windows features on or off'")
            print("   - Enable: Hyper-V, Windows Subsystem for Linux (WSL)")
            print("   - Restart computer after enabling")
            print()
            print("4. **Run as Administrator**:")
            print("   - Right-click Command Prompt")
            print("   - Select 'Run as administrator'")
            print("   - Try Docker commands again")
            print()
            print("5. **Check Docker Settings**:")
            print("   - Open Docker Desktop")
            print("   - Go to Settings > General")
            print("   - Ensure 'Use WSL 2 based engine' is checked")
            print()
            print("6. **Alternative: Use Simple Kafka (option 7)")
            
        elif choice == "7":
            # Use Simple Kafka
            print("\n🚀 Starting Simple Kafka Pipeline...")
            try:
                import subprocess
                result = subprocess.run([sys.executable, "run_simple_kafka_pipeline.py"], 
                                      capture_output=False)
                if result.returncode == 0:
                    print("✅ Simple Kafka pipeline completed")
                else:
                    print("❌ Simple Kafka pipeline failed")
            except Exception as e:
                print(f"❌ Error running simple Kafka: {e}")
                
        elif choice == "8":
            print("👋 Exiting Kafka setup")
            break
            
        else:
            print("❌ Invalid choice")


if __name__ == "__main__":
    main()