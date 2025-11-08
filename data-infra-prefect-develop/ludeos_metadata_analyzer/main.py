from collections import deque

from concurrent.futures import ThreadPoolExecutor, as_completed
from prefect import flow, task, get_run_logger

from data_utils.pythonic_utils import read_file_content
from data_utils.claude_utils import get_image_from_url

from drivers import claude_driver
from ludeos_metadata_analyzer.utils import save_results_to_snowflake, fetch_ludeos_from_snowflake

MAX_WORKERS = 5
DEFAULT_TEMPERATURE = 0.3
SNOWFLAKE_BATCH_SIZE = 10
VIDEO_ANALYSIS_FRAME_INTERVAL = 3
VIDEO_ANALYSIS_MAX_FRAMES = 5
VIDEO_ANALYSIS_WORDS_BACK = 20


@task
def process_single_ludeo(ludeo, i, total):
    logger = get_run_logger()

    video_context = read_file_content('ludeos_metadata_analyzer/context/video.txt')
    single_frame_context = read_file_content('ludeos_metadata_analyzer/context/images.txt')
    tags_context = read_file_content('ludeos_metadata_analyzer/context/tags.txt')
    objective_and_score_params = read_file_content('ludeos_metadata_analyzer/context/objective_and_score_params.txt')
    environment = read_file_content('ludeos_metadata_analyzer/context/environment.txt')

    ludeo_id = ludeo.pop('ludeo_id')
    ludeo_name = ludeo.pop('ludeo_name')
    img_url = ludeo.pop('thumb_image')
    video_url = ludeo.pop('webm_video_link')
    objective = ludeo.pop('objective')
    score_params = ludeo.pop('score_params')

    logger.info(f"\nProcessing ludeo {i + 1}/{total}: {ludeo_id} - {ludeo_name}")

    try:
        thumbnail_img_params = get_image_from_url(img_url)

        logger.info('analyzing thumbnail image')
        thumbnail_img_analysis = claude_driver.analyze_image(
            image_path=thumbnail_img_params['file_path'],
            prompt=single_frame_context,
            temperature=DEFAULT_TEMPERATURE,
            words_back=10,
            media_type=thumbnail_img_params['media_type']
        )
    except Exception as err:
        logger.warning(f'failed to analyze thumbnail image. error: {err}. skipping')
        thumbnail_img_analysis = None

    try:
        logger.info('analyzing video')
        video_analysis = claude_driver.analyze_video(
            video_prompt=video_context,
            single_frame_prompt=single_frame_context,
            video_path_or_url=video_url,
            frame_interval=VIDEO_ANALYSIS_FRAME_INTERVAL,
            max_frames=VIDEO_ANALYSIS_MAX_FRAMES,
            summarize_frames=True,
            words_back=VIDEO_ANALYSIS_WORDS_BACK,
            temperature=DEFAULT_TEMPERATURE
        )
    except Exception as err:
        logger.warning(f'failed to analyze ludeo video. error: {err}')
        video_analysis = None

    try:
        logger.info('analyzing objective and score params')
        objective_score_params_analysis = claude_driver.get_response(
            prompt=objective_and_score_params.format(objective=objective, score_params=score_params),
            temperature=DEFAULT_TEMPERATURE
        )
    except Exception as err:
        logger.warning(f'failed to analyze objective and score params. error: {err}')
        objective_score_params_analysis = None

    try:
        logger.info('analyzing tags')
        tags_analysis = claude_driver.get_response(
            prompt=tags_context.format(
                video_analysis=video_analysis,
                objective_score_params_analysis=objective_score_params_analysis
            ),
            temperature=DEFAULT_TEMPERATURE
        )
    except Exception as err:
        logger.warning(f'failed to analyze tags. error: {err}')
        tags_analysis = None

    try:
        logger.info('analyzing tags')
        environment_analysis = claude_driver.get_response(
            prompt=environment.format(video_analysis=video_analysis),
            temperature=DEFAULT_TEMPERATURE
        )
    except Exception as err:
        logger.warning(f'failed to analyze tags. error: {err}')
        environment_analysis = None

    result = {
        'ludeo_id': ludeo_id,
        'ludeo_name': ludeo_name,
        'game_name': ludeo.pop('game_name'),
        'img_url': img_url,
        'thumbnail_img_analysis': thumbnail_img_analysis,
        'video_url': video_url,
        'video_analysis': video_analysis,
        'objective_score_params_analysis': objective_score_params_analysis,
        'tags': tags_analysis,
        'environment_analysis': environment_analysis,
        'original_data': ludeo
    }

    logger.info(f"✅ Done with {ludeo_id} | tags: {tags_analysis}")
    return result


@task()
def analyze_ludeos(ludeos_to_analyze):
    logger = get_run_logger()
    if not ludeos_to_analyze:
        logger.info("No ludeos found to analyze.")
        return []

    analyzed_ludeos = []
    results_buffer = deque()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(process_single_ludeo, ludeo, i, len(ludeos_to_analyze)): i
            for i, ludeo in enumerate(ludeos_to_analyze)
        }

        for future in as_completed(futures):
            try:
                result = future.result()
                results_buffer.append(result)
                analyzed_ludeos.append(result)

                if len(results_buffer) >= SNOWFLAKE_BATCH_SIZE:
                    logger.info(f"Saving batch of {SNOWFLAKE_BATCH_SIZE} results to Snowflake...")
                    save_results_to_snowflake(list(results_buffer))
                    results_buffer.clear()

            except Exception as e:
                logger.warning(f"❌ Error in ludeo processing: {e}")

    if results_buffer:
        logger.info(f"Saving final batch of {len(results_buffer)} results to Snowflake...")
        save_results_to_snowflake(list(results_buffer))

    return analyzed_ludeos


@flow
def ludeos_metadata_analyzer_flow():
    logger = get_run_logger()
    logger.info('starting Ludeo Video Analyzer flow')
    logger.info("=== Ludeo Game Image Analysis System ===")

    ludeos_to_analyze = fetch_ludeos_from_snowflake()
    results = analyze_ludeos(ludeos_to_analyze)
    if results:
        logger.info(f"\nCompleted analysis of {len(results)} ludeos")
        save_results_to_snowflake(results)
    else:
        logger.info("No ludeos were analyzed. Check for errors above.")


if __name__ == '__main__':
    ludeos_metadata_analyzer_flow()
