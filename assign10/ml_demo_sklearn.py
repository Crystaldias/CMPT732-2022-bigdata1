import numpy as np
import pandas as pd

from sklearn.model_selection import train_test_split
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import FunctionTransformer
from sklearn.ensemble import GradientBoostingRegressor, GradientBoostingClassifier


def create_data(y_col='volume'):
    n = 100000
    data = pd.DataFrame({
        'length': np.random.rand(n)*100 - 50,
        'width': np.random.rand(n)*100 - 50,
        'height': np.random.rand(n)*100 - 50,
    })
    data['volume'] = data['length'] * data['width'] * data['height']
    data['v_sign'] = data['volume'] > 0
    
    X = data[['length', 'width', 'height']]
    y = data[y_col]
    return train_test_split(X, y)    


def regression():
    X_train, X_test, y_train, y_test = create_data()
    
    model = GradientBoostingRegressor(n_estimators=100, max_depth=5)
    model.fit(X_train, y_train)

    predictions = model.predict(X_test)
    print(pd.DataFrame({'target': y_test, 'prediction': predictions}))
    print(model.score(X_test, y_test))


def to_sign(X):
    return (X > 0).astype(int)


def classification():
    X_train, X_test, y_train, y_test = create_data(y_col='v_sign')
    
    model = make_pipeline(
        FunctionTransformer(to_sign),
        GradientBoostingClassifier(n_estimators=100, max_depth=5)
    )
    model.fit(X_train, y_train)

    predictions = model.predict(X_test)
    print(pd.DataFrame({'target': y_test, 'prediction': predictions}))
    print(model.score(X_test, y_test))


if __name__ == '__main__':
    regression()
    classification()