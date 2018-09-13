# -*- coding: utf-8 -*-

import datetime, os, sys, time
from threading import Thread

import ee

class Task():
	def __init__(self, code, specifications):
		self.__code = code
		self.__specifications = specifications
		self.__gee_task = None

	@property
	def code(self):
		return self.__code

	@property
	def state(self):
		if self.__gee_task != None:
			return self.__gee_task.status()['state']
		else:
			return None

	@property
	def error_message(self):
		if self.__gee_task != None:
			return self.__gee_task.status()['error_message']
		else:
			return None

	@property
	def specifications(self):
		return self.__specifications

	@property
	def gee_task(self):
		return self.__gee_task

	def set_gee_task(self, gee_task):
		self.__gee_task = gee_task

	def start(self):
		self.__gee_task.start()

	def __eq__(self, other):
		"""Overrides the default implementation"""
		if isinstance(other, Task):
			return self.code == other.code
		return NotImplemented

class TaskManager(Thread):
	def __init__(self, export_class, max_tasks, interval, max_errors=3):
		Thread.__init__(self)

		self.__export_class = export_class
		self.__max_tasks = max_tasks
		self.__interval = interval
		self.__max_errors = max_errors

		self.__tasks_awaiting = {}
		self.__tasks_running = {}
		self.__tasks_completed = {}
		self.__tasks_error = {}
		self.__tasks_failed = {}

	def run(self):
		def process_tasks(tasks):
			while len(tasks) > 0 or len(self.__tasks_running) > 0:
				try:
					self.__print()
					self.__submit_task(tasks)
					self.update_tasks()
					time.sleep(self.__interval)
				except Exception as e:
					print("Exception: {}".format(e))

		process_tasks(self.__tasks_awaiting)
		print("Finished!!!")
		sys.exit(0)

	def get_export_class(self):
		return self.__export_class

	def add_task(self, code, specifications):
		task = Task(code=code, specifications=ee.serializer.toJSON(specifications))
		self.__tasks_awaiting[code] = task

	def update_tasks(self):
		for code, task in self.__tasks_running.copy().items():
			
			remote_state = task.state
			remote_info = None

			if remote_state == ee.batch.Task.State.UNSUBMITTED:
				try:
					task.start()
					print("Task {code} started!".format(code=task.code))
					remote_state = ee.batch.Task.State.READY
				except Exception as e:
					print(e)
					del self.__tasks_running[code]
					remote_info = t.error_message
					remote_state = ee.batch.Task.State.FAILED

					self.__tasks_error[code] = self.__tasks_error[code] + 1 if self.__tasks_error.has_key(code) else 0
					if self.__tasks_error[code] <= self.__max_errors:
						self.__tasks_failed[code] = True

			elif remote_state == ee.batch.Task.State.COMPLETED:
				del self.__tasks_running[task.code]
				self.__tasks_completed[task.code] = True

			elif remote_state in [ee.batch.Task.State.CANCELLED, ee.batch.Task.State.CANCEL_REQUESTED]:
				del self.__tasks_running[task.code]

			elif remote_state == ee.batch.Task.State.FAILED:
				remote_info = task.error_message

				if remote_info.find("No valid training data were found") != -1 or remote_info.find("Internal error") != -1:
					remote_state = ee.batch.Task.State.CANCELLED
					self.__tasks_failed[code] = True
					del self.__tasks_running[task.code]
				else:
					self.__tasks_error[code] = self.__tasks_error[code] + 1 if self.__tasks_error.has_key(code) else 1

					if self.__tasks_error[code] <= self.__max_errors:
						remote_state = ee.batch.Task.State.UNSUBMITTED
						self.__tasks_running[code] = self.__generate_task(task)
					else:
						self.__tasks_failed[code] = True
						del self.__tasks_running[code]

	def __print(self):
		os.system('clear')
		print("************************* Tasks *************************")
		print("Awaiting:    {0} tasks".format(len(self.__tasks_awaiting)))
		print("Running:     {0} tasks".format(len(self.__tasks_running)))
		print("Completed:   {0} tasks".format(len(self.__tasks_completed)))
		print("Error:       {0} tasks".format(len(self.__tasks_error)))
		print("Failed:      {0} tasks".format(len(self.__tasks_failed)))
		print("*********************************************************")
		for code, task in self.__tasks_running.copy().items():
			print(code, "|", task.state)

	def __generate_task(self, task):
		if task:
			specifications = ee.deserializer.fromJSON(task.specifications)
		else:
			raise AttributeError("Task not found")
		task.set_gee_task(self.__export_class(**specifications))
		return task

	def __submit_task(self, tasks):
		for code, task in tasks.copy().items():
			if code in self.__tasks_failed.keys():
				del tasks[code]
				continue

			if len(self.__tasks_running) >= self.__max_tasks or len(tasks) == 0:
				break

			if task.state in [ee.batch.Task.State.COMPLETED, ee.batch.Task.State.CANCELLED]:
				del tasks[code]
			if task.state in [None, ee.batch.Task.State.UNSUBMITTED, ee.batch.Task.State.FAILED]:
				new_task = self.__generate_task(task)
				self.__tasks_running[code] = new_task
				del tasks[code]
			if task.state in [ee.batch.Task.State.READY, ee.batch.Task.State.RUNNING]:
				print("{0} running in other process".format(code))
				del tasks[code]
