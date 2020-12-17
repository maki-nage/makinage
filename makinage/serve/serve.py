import zipfile
import io
import tempfile
import traceback
from collections import namedtuple


import rx.operators as ops
import rxsci as rs
from cyclotron.debug import trace_observable
from makinage.util import import_function

from mlflow.pyfunc import load_model
from mlflow.pyfunc.backend import PyFuncBackend
import numpy as np

Transforms = namedtuple('Transforms', ['pre', 'post'])


def load_mlflow_model(data):
    with tempfile.TemporaryDirectory() as tmp:
        data = io.BytesIO(data)
        with zipfile.ZipFile(data) as artifact:
            artifact.extractall(path=tmp)
            model = load_model(tmp)
            return model


def create_model_predict(model, config):
    print("creating inference: {}".format(type(model)))
    if 'predict' in config['config']['serve']:
        print("customizing inference with: {}".format(config['config']['serve']['predict']))
        predict = import_function(config['config']['serve']['predict'])
        predict = predict(model, config)
    else:
        try:
            predict = model.keras_model.predict  # temporary until mlflow #2830
        except Exception:
            predict = model.predict

    return predict


def infer(data, transforms, predict):
    try:
        prediction = transforms.pre(data)
        if prediction is not None:
            prediction = predict(prediction)
        if prediction is not None:
            prediction = transforms.post(data, prediction)
        return prediction
    except Exception as e:
        print("infer error: {}".format(e))
        print(traceback.print_tb(e.__traceback__))
        return None


def create_transform_functions(config):
    if 'pre_transform' in config['config']['serve']:
        pre_transform = import_function(config['config']['serve']['pre_transform'])
        pre_transform = pre_transform(config)
    else:
        pre_transform = np.array

    if 'post_transform' in config['config']['serve']:
        post_transform = import_function(config['config']['serve']['post_transform'])
        post_transform = post_transform(config)
    else:
        def post_transform(utterance, prediction): return utterance, prediction

    return Transforms(pre_transform, post_transform)


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
        trace_observable(prefix="model", trace_next_payload=False),
        ops.map(load_mlflow_model),
        ops.combine_latest(config),
        ops.starmap(create_model_predict),
    )

    transforms = config.pipe(
        trace_observable(prefix="prepare", trace_next_payload=False),
        ops.map(create_transform_functions)
    )

    prediction = data.pipe(
        rs.ops.with_latest_from(transforms, predict),
        ops.starmap(infer),
        ops.filter(lambda i: i is not None),
    )

    return prediction,
