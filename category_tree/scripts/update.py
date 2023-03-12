import datetime
import logging
import os
import shutil

from category_tree.data_dir import DataDir, DEFAULT_DATA_DIR

languages = (
    'en',
    'ceb',
    'de',
    'sv',
    'fr',
    'nl',
    'ru',
    'es',
    'it',
    'arz',
    'pl',
    'ja',
    'zh',
    'vi',
    # 'war',
    'uk',
    'ar',
    'pt',
    'fa',
    'ca',
    'sr',
    # 'id',
    'ko',
    'no',
    'ce',
    'fi',
    'tr',
    'hu',
    'cs',
    'tt',
    'sh',
    'ro',
    # 'zh-min-nan',
    'eu',
    'ms',
    'eo',
)


def update(language: str):
    data_dir = DataDir(language)

    data_dir.save_raw_category_tree()
    data_dir.save_trimmed_category_tree()
    data_dir.save_compressed_category_tree()
    data_dir.save_meta_file()


def update_all():
    output_dir = DEFAULT_DATA_DIR

    shutil.rmtree(output_dir)
    os.mkdir(output_dir)

    for lang in languages:
        starting_time = datetime.datetime.now()

        logging.info(f"Starting {lang}wiki at {starting_time.time()}")

        try:
            update(lang)
        except Exception as e:
            logging.exception(e, exc_info=e, stack_info=True)

        logging.info(f"Finished in {datetime.datetime.now() - starting_time}")
