import datetime
import gzip
import hashlib
import json
import os.path
import pathlib
from typing import Optional

from category_tree.category_tree import CategoryTree
from category_tree.deserialize import deserialize
from category_tree.generate.fetch_category_tree_data import fetch_category_tree_data
from category_tree.serialize import serialize

DEFAULT_DATA_DIR = pathlib.Path(__file__).parent.parent.parent.parent.joinpath("data", "category_tree_data").absolute()


class DataDirPathProvider:
    data_dir_path: pathlib.Path
    language: str

    def __init__(self, data_dir_path: pathlib.Path, language: str):
        self.data_dir_path = data_dir_path
        self.language = language

    def raw_category_tree_path(self):
        return self.data_dir_path.joinpath(self.language, f"{self.language}_raw_category_tree.json").absolute()

    def trimmed_category_tree_path(self):
        return self.data_dir_path.joinpath(self.language, f"{self.language}_trimmed_category_tree.json").absolute()

    def compressed_category_tree_path(self):
        return self.data_dir_path.joinpath(self.language, f"{self.language}_category_tree.json.gz").absolute()

    def meta_file_path(self):
        return self.data_dir_path.joinpath(self.language, "meta.json").absolute()


class DataDir:
    """
    Specifies default behavior for interacting with the data repository.
    """

    language: str
    path_provider: DataDirPathProvider

    def __init__(
            self,
            language: str,
            *,
            data_dir_path: pathlib.Path = DEFAULT_DATA_DIR,
            path_provider: DataDirPathProvider = None):

        self.language = language
        self.path_provider = path_provider if path_provider is not None else \
            DataDirPathProvider(data_dir_path, language)

    def _ensure_dirs_exists(self):
        required_paths = (
            x.parent for x in (
                self.raw_category_tree_path,
                self.trimmed_category_tree_path,
                self.compressed_category_tree_path,
                self.meta_file_path
            )
        )

        for path in required_paths:
            if not os.path.exists(path):
                os.mkdir(path)

    @property
    def raw_category_tree_path(self):
        return self.path_provider.raw_category_tree_path()

    @property
    def trimmed_category_tree_path(self):
        return self.path_provider.trimmed_category_tree_path()

    @property
    def compressed_category_tree_path(self):
        return self.path_provider.compressed_category_tree_path()

    @property
    def meta_file_path(self):
        return self.path_provider.meta_file_path()

    def save_raw_category_tree(self):
        self._ensure_dirs_exists()

        category_tree_data = fetch_category_tree_data(self.language)
        serialize(category_tree_data, self.raw_category_tree_path)

    def save_trimmed_category_tree(self, pages_percentile: int = 65, max_depth: Optional[int] = 100):
        self._ensure_dirs_exists()

        raw_tree_data = deserialize(self.raw_category_tree_path)
        cat_tree = CategoryTree(raw_tree_data)

        cat_tree.trim_by_page_count_percentile(pages_percentile)
        cat_tree.trim_by_id_without_name()
        cat_tree.trim_by_max_depth(max_depth)

        with open(self.trimmed_category_tree_path, 'wb') as f:
            content = json.dumps(cat_tree.to_dict(), ensure_ascii=False, indent=1)
            f.write(content.encode("utf-8"))

    def save_compressed_category_tree(self):
        self._ensure_dirs_exists()

        with gzip.open(self.compressed_category_tree_path, 'w') as f_out, \
                open(self.trimmed_category_tree_path, 'rb') as f_in:
            f_out.write(f_in.read())

    def save_meta_file(self):
        self._ensure_dirs_exists()

        meta_dict = deserialize(self.raw_category_tree_path).to_dict()["meta"]

        with gzip.open(self.compressed_category_tree_path, 'r') as f:
            data = f.read()
            sha256hash = hashlib.sha256(data).hexdigest()
            meta_dict["uncompressed_sha256"] = sha256hash

        meta_dict["updated"] = datetime.date.today().isoformat()

        with open(self.meta_file_path, 'w') as f_out:
            json.dump(meta_dict, f_out, indent=1)
