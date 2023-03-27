"""Tasks running the core analyses."""

import pandas as pd
import pytask

from di_tella_2004_replication.config import BLD


@pytask.mark.depends_on(
    {
        "scripts": ["crime_by_block_analysis.py", "predict.py"],
        "CrimeByBlock": BLD / "python" / "data" / "CrimeByBlock_Panel.pkl",
    },
)
@pytask.mark.produces(BLD / "python" / "models" / "model.pickle")
def task_fit_model_python(depends_on, produces):
    data_info = read_yaml(depends_on["data_info"])
    data = pd.read_csv(depends_on["data"])
    model = fit_logit_model(data, data_info, model_type="linear")
    model.save(produces)

    @pytask.mark.depends_on(
        {
            "data": BLD / "python" / "data" / "data_clean.csv",
            "model": BLD / "python" / "models" / "model.pickle",
        },
    )
    @pytask.mark.task(id=group, kwargs=kwargs)
    def task_predict_python(depends_on, group, produces):
        model = load_model(depends_on["model"])
        data = pd.read_csv(depends_on["data"])
        predicted_prob = predict_prob_by_age(data, model, group)
        predicted_prob.to_csv(produces, index=False)
