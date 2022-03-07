from functools import reduce


class DataPipeline:
    def __init__(self, data=None, clean_func=None, generate_func=None, select_func=None):
        self.data = data

        self.clean_func = clean_func
        self.generate_func = generate_func
        self.select_func = select_func

    @staticmethod
    def compose(*functions):
        return reduce(lambda f, g: lambda x: f(g(x)), functions, lambda x: x)

    def clean_data(self):
        self.data = self.clean_func(self.data)

    def generate_data(self):
        self.data = self.generate_func(self.data)

    def select_data(self):
        self.data = self.select_func(self.data)

    def run_label(self):
        self.clean_data()
        self.generate_data()
        self.clean_data()
        return self.data

    def run_feature(self):
        self.clean_data()
        self.generate_data()
        self.select_data()
        self.clean_data()
        return self.data
