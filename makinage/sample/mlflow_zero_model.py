import pickle
import mlflow.pyfunc


class ZeroWrapper(mlflow.pyfunc.PythonModel):
    def load_context(self, context):
        with open(context.artifacts["zero_model"], 'rb') as fd:
            self.model = pickle.load(fd)

    def predict(self, context, model_input):
        return self.model.predict(model_input)
