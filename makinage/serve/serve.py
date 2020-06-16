import zipfile
import io
import tempfile

import rx.operators as ops
import rxsci as rs
from makinage.util import import_function

from mlflow.pyfunc import load_model
from mlflow.pyfunc.backend import PyFuncBackend
import numpy as np


def load_model(data):
    with tempfile.TemporaryDirectory() as tmp:
        with zipfile.ZipFile(io.BytesIO(data)) as artifact:
            artifact.extractall(path=tmp)
            model = load_model(tmp)
            return model


def create_model_predict(model):
    #return model.predict
    return model.keras_model.predict # temporary until mlflow #2830


def predict(data, config, predict):
    prediction = predict(data[config['config']['serve']['input_field']])
    prediction = prediction.tolist()
    data[config['config']['serve']['output_field']] = prediction
    return data


def create_prepare_function(config):
    if 'prepare' in config['config']['serve']:
        prepare = import_function(config['config']['serve']['prepare'])
    else:
        prepare = np.array

    return prepare


def serve(config, model, data):
    '''Serves a model

    This operator serves a model. It loads models received on the model
    observable, and executes it on each item received on the data observable. 

    The configuration observable must contain a serve section with the following
    fields:

    * input_field: The input field name used to run inference.
    * output_field: The output field name where inference result is set.

    additionally, a "prepare" field can be set if some data transformation is
    needed before feeding the model. When not present, the input data is
    converted to a numpy array

    Args:
        config: configuration observable.

    Returns:
        An observable of predictions. Each item is a copy of the original datay
        item, with an additional field. The name of the additional field if the
        one set in output_field.
    '''
    predict = model.pipe(
        ops.map(load_model),
        ops.map(create_model_predict),
    )

    prepare = config.pipe(
        ops.map(create_prepare_function)
    )

    prediction = data.pipe(
        rs.with_latest_from(prepare),
        ops.map(lambda i: i[1](i[0])),
        rs.with_latest_from(config, predict),
        ops.starmap(predict),
    )

    return prediction
