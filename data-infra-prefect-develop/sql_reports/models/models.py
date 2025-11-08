from sqlalchemy import Column, String, Integer, DateTime, Boolean, Enum, ARRAY, Text
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()


REPORT_KIND_CHOICES = ('dqa', 'technical', 'business')
TEAM_CHOICES = ('data_engineers', 'platform', 'unit', 'analysts', 'data_science', 'ludeo_hub')


class SqlReportsMng(Base):
    __tablename__ = 'sql_reports_mng'

    id = Column(Integer, primary_key=True)
    report_name = Column(String, nullable=False, unique=True)
    report_display_name = Column(String, nullable=False)
    cron = Column(String, nullable=False)
    last_successful_run = Column(DateTime, nullable=True)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    owner = Column(String, nullable=False)
    report_kind = Column(Enum(*REPORT_KIND_CHOICES, name="report_kind_enum"), nullable=True, default="dqa")
    team = Column(Enum(*TEAM_CHOICES, name="team_enum"), nullable=True, default="analysts")
    slack_recipients = Column(ARRAY(String), nullable=True)
    email_recipients = Column(ARRAY(String), nullable=True)
    query = Column(Text, nullable=True)
