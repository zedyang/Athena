__author__ = 'zed'


def singleton(cls_):
    """
    Implementation of singleton class decorator.
    :param cls_: Python class.
    :return: get_instance function of this class.
    """
    instances = dict()

    def get_instance(*args, **kwargs):
        if cls_ not in instances:
            instances[cls_] = cls_(*args, **kwargs)
        return instances[cls_]
    return get_instance
