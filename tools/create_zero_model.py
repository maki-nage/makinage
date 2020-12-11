import os
import pickle
import mlflow

from makinage.sample.mlflow_zero_model import ZeroWrapper
from makinage.sample.serve import ZeroModel


def main():

    xgb_model_path = "/tmp/zero.pkl"
    model = ZeroModel()
    with open('/tmp/zero.pkl', 'wb') as fd:
        pickle.dump(model, fd)

    #mlflow.set_tracking_uri(os.environ['MLFLOW_ENDPOINT'])
    #experiment_id = get_experiment("makinage_zero")
    #with mlflow.start_run(experiment_id=experiment_id):
    artifacts = {
        "zero_model": xgb_model_path
    }

    mlflow_pyfunc_model_path = "/tmp/zero_mlflow_pyfunc"
    mlflow.pyfunc.save_model(
            path=mlflow_pyfunc_model_path,
            python_model=ZeroWrapper(),
            artifacts=artifacts,
            #conda_env=conda_env
    )


if __name__ == '__main__':
    main()
