# Makefile for ColdChainIQ / FrostGuard

.PHONY: up down migrate consumer-up consumer-logs test-consumer test-all rebuild-redis verify-phase1 verify-phase2

# Start foundational infrastructure (Postgres, Redis, Redpanda, Console)
up:
	docker compose up -d postgres redis redpanda console

# Tear down all containers and networks
down:
	docker compose down

# Execute database schema migrations via Alembic
migrate:
	docker compose run --rm migrate

# Start consumer service in background
consumer-up:
	docker compose up -d consumer

# Follow live consumer logs
consumer-logs:
	docker compose logs -f consumer

# Run consumer unit tests
test-consumer:
	pytest services/consumer/tests -v

# Run entire test suite (Simulator + Consumer)
test-all:
	pytest services/simulator/tests services/consumer/tests -v

# Rebuild Redis latest state and rolling window from PostgreSQL
rebuild-redis:
	python -c "from consumer.config import ConsumerSettings; from consumer.redis_store import RedisStore, rebuild_redis_from_postgres; from consumer.repository import PostgresRepository; s = ConsumerSettings(); repo = PostgresRepository(s); r_store = RedisStore(s); count = rebuild_redis_from_postgres(repo.session_factory(), r_store, minutes=s.WINDOW_MINUTES); print(f'Rebuilt {count} readings in Redis.')"

# Run automated Phase 1 QA verification
verify-phase1:
	python services/simulator/scripts/verify_phase1.py --all

# Run automated Phase 2 QA verification
verify-phase2:
	python services/consumer/scripts/verify_phase2.py --all
