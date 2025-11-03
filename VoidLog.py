
# Default value for parameters which accept a logger.
class VoidLog():
	def debug(self, msg):
		pass
	def info(self, msg):
		pass
	def warning(self, msg):
		pass
	def error(self, msg):
		pass
	def critical(self, msg):
		pass