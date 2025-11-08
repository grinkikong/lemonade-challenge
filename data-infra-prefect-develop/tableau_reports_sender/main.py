import os
import shutil

from prefect import flow, task, get_run_logger
from data_utils.tableau_utils import TableauDriver
from data_utils.chatgpt_utils import ChatGPTDriver
from data_utils.slack_utils import SlackDriver
from data_utils.aws_secrets_manager_utils import AWSSecretsManagerDriver
from config import ENV, TABLEAU_REPORTS_CONFIG, CONTEXT_FILE_PATH, SYSTEM_ROLE, TABLEAU_IMAGES_DIR

# Secrets
secrets_manager_driver = AWSSecretsManagerDriver(env=ENV)
tableau_secrets = secrets_manager_driver.get_secret("tableau/api-credentials")
tbl_api_token_name = tableau_secrets["token_name"]
tbl_api_token_secret = tableau_secrets["token_secret"]

chatgpt_secrets = secrets_manager_driver.get_secret("chatgpt/api-credentials")
chatgpt_api_key_secret = chatgpt_secrets["api_key"]

slack_secrets = secrets_manager_driver.get_secret("slack/ludata_bot")

# Drivers
tableau_driver = TableauDriver(tbl_api_token_name, tbl_api_token_secret)
gpt_driver = ChatGPTDriver(chatgpt_api_key_secret)
slack_driver = SlackDriver(slack_secrets["token"])


@task()
def _create_temp_tableau_imgs_dir():
    logger = get_run_logger()
    os.makedirs(TABLEAU_IMAGES_DIR, exist_ok=True)
    logger.info(f"Created temp tableau images directory: {TABLEAU_IMAGES_DIR}")


@task()
def send_to_slack(report_config, report_img, gpt_analysis):
    logger = get_run_logger()
    slack_channels = report_config.get_slack_channels()
    title = report_config.tableau_report_name

    for channel in slack_channels:
        logger.info(f"Sending Tableau Report to Slack ({channel}) for {title}...")
        if gpt_analysis:
            logger.info(f"Sending GPT analysis to slack")
            title = f"{title} - Analysis"
            msg = slack_driver.generate_slack_message_payload(
                title=title, text=gpt_analysis, color="success"
            )
            slack_driver.send_message(channel, msg)

        logger.info(f"Sending Image to Slack ({channel}) for {title}...")
        file = f"{TABLEAU_IMAGES_DIR}/{title}.png"
        slack_driver.upload_file(file_content=report_img, file_name=file, channels=channel, title=title)


def _download_tableau_report_as_image(report_name, all_tableau_views):
    logger = get_run_logger()
    base_path = f"{TABLEAU_IMAGES_DIR}"
    report_img = tableau_driver.download_view(
        views=all_tableau_views, view_name=report_name, file_type="image", path=base_path
    )
    logger.info(f"Successfully downloaded image from tableau server")
    return report_img


def _analyze_report_with_chatgpt(report_name, user_context):
    logger = get_run_logger()
    logger.info(f"Analyzing report image in chatGPT")
    try:
        file = f"{TABLEAU_IMAGES_DIR}/{report_name}.png"
        gpt_analysis = gpt_driver.analyze_image(file, SYSTEM_ROLE, user_context)
        logger.info(f"Analyzing report completed")
    except Exception as e:
        logger.error(f"Error analyzing report with ChatGPT: {e}")
        gpt_analysis = None
    return gpt_analysis


@task()
def run_process_for_single_report(report_config, user_context, all_tableau_views):
    logger = get_run_logger()
    report_name = report_config.tableau_report_name
    report_img = _download_tableau_report_as_image(report_name, all_tableau_views)
    if report_config.analyze_with_chatgpt:
        gpt_analysis = _analyze_report_with_chatgpt(report_name, user_context)
    else:
        logger.info(f"skipping GPT analysis (analyze_with_chatgpt=False)")
        gpt_analysis = None
    send_to_slack(report_config, report_img, gpt_analysis)


@task()
def _delete_temp_tableau_imgs_dir():
    logger = get_run_logger()
    if os.path.exists(TABLEAU_IMAGES_DIR):
        shutil.rmtree(TABLEAU_IMAGES_DIR)
        logger.info(f"Deleted temp directory: {TABLEAU_IMAGES_DIR}")
    else:
        logger.warning(f"Temp directory not found: {TABLEAU_IMAGES_DIR}")


@flow()
def tableau_reports_sender_flow():
    logger = get_run_logger()
    logger.info("Starting the report analyzer flow...")
    if ENV == 'staging':
        logger.info("Running in staging environment, skipping tableau report generation.")
        return
    with open(CONTEXT_FILE_PATH, "r", encoding="utf-8") as file:
        user_context = file.read().strip()

    _create_temp_tableau_imgs_dir()
    all_tableau_views = tableau_driver.get_all_views()
    for report_config in TABLEAU_REPORTS_CONFIG:
        try:
            logger.info(f"Running on report: {report_config.tableau_report_name}")
            run_process_for_single_report(report_config, user_context, all_tableau_views)
            logger.info(f"Successfully processed report {report_config.tableau_report_name}")
        except Exception as e:
            logger.error(f"Error processing report {report_config.tableau_report_name}: {e}")
            continue

    _delete_temp_tableau_imgs_dir()
    logger.info("Report analyzer flow completed!")


if __name__ == "__main__":
    tableau_reports_sender_flow()
