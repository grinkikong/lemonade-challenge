import os
from dataclasses import dataclass
from typing import List

ENV = os.environ['ENV']

DEFAULT_SLACK_CHANNEL = ["#monitoring-data-sandbox"]
TABLEAU_IMAGES_DIR = 'tmp_tableau_imgs'
CONTEXT_FILE_PATH = "tableau_reports_sender/context_file.txt"
SYSTEM_ROLE = "you are an analyst that analyzes dashboards and provides insights"


@dataclass
class TableauReportConfig:
    tableau_report_name: str
    slack_channels: List[str]
    owner: str
    analyze_with_chatgpt: bool

    def get_slack_channels(self) -> List[str]:
        return self.slack_channels if ENV == 'production' else DEFAULT_SLACK_CHANNEL


TABLEAU_REPORTS_CONFIG = [
    TableauReportConfig(
        tableau_report_name="GameCast Daily Report",
        slack_channels=["#data-reports"],
        owner="ben.tohami@ludeo.com",
        analyze_with_chatgpt=True,
    ),
    TableauReportConfig(
        tableau_report_name="Players XP Daily Report",
        slack_channels=["#data-reports"],
        owner="omer.eldadi@ludeo.com",
        analyze_with_chatgpt=True,
    ),
    TableauReportConfig(
        tableau_report_name="Data Engineers KPIs",
        slack_channels=["#data-engineering-notifications"],
        owner="shay.misgav@ludeo.com",
        analyze_with_chatgpt=False,
    ),
    TableauReportConfig(
        tableau_report_name="Main View ",
        slack_channels=["#data-reports"],
        owner="ben.tohami@ludeo.com",
        analyze_with_chatgpt=True,
    )
]