class A:
	def __init__(self, abc, *args, **kwargs):
		print(args, abc, kwargs)

class B:
	def __init__(self, *args, ben, **kwargs):
		print(ben)

class C(A, B):
	pass

C(1, ben=2)