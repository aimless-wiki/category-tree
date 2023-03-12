import logging

from category_tree.scripts.update import update_all, update


if __name__ == '__main__':
    logging.root.setLevel(logging.INFO)
    update_all()
