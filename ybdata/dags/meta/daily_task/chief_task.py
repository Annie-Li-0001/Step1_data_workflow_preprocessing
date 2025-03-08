"""
文件说明
"""

from meta.daily_task.task_store import *
from meta.daily_task.factor_store import *


#  所有数据同步类的代表，用以在airflow的dag文件中作为调用者
class ChiefDataTask(metaclass=TaskMeta):
    def __filter_task(self, cond: Callable):
        tasks = list(filter(cond, self.__data_task_stores__))
        return tasks

    def filter_daily_tasks(self):
        return self.__filter_task(lambda x: x.freq == Freq.daily.value)

    def filter_weekly_tasks(self):
        return self.__filter_task(lambda x: x.freq == Freq.weekly.value)
