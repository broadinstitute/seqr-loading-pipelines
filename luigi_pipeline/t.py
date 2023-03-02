def deco(f):
	def inner(self):
		return f(self)
	return inner

class Ben():

	@deco
	def method(self):
		print("hi")



a = Ben().method

print(a(), dir(a))