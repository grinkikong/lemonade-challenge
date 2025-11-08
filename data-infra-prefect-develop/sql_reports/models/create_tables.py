from models import Base, SqlReportsMng
from drivers import postgres_driver  # Your existing SQLAlchemy connection


def create_tables():
    engine = postgres_driver._get_engine()  # Use the existing database engine
    Base.metadata.create_all(bind=engine)  # Create tables based on all registered models
    print("Tables created successfully.")


if __name__ == "__main__":
    create_tables()
