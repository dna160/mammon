.PHONY: up down build logs test clean

up:
	cd project_sniper && docker compose up --build

up-d:
	cd project_sniper && docker compose up --build -d

down:
	cd project_sniper && docker compose down -v

build:
	cd project_sniper && docker compose build

logs:
	cd project_sniper && docker compose logs -f

test:
	pip install pytest numpy numba psycopg2-binary redis && \
	NUMBA_DISABLE_JIT=1 pytest project_sniper/tests/ -v

clean:
	cd project_sniper && docker compose down -v --rmi local
