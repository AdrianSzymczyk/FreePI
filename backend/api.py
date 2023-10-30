import json
from datetime import datetime
from functools import wraps
from http import HTTPStatus
from typing import Dict

from fastapi import FastAPI, Request, HTTPException

from webScrape import receiver
from backend import technical_indicators

tags_metadata = [
    {
        'name': 'General',
        'description': 'Default endpoint with connection information'
    },
    {
        'name': 'Daily',
        'description': 'This API endpoint returns **daily** time series of the stock symbol data.'
                       '**Function parameter** = _TIME_SERIES_DAILY_'
    },
    {
        'name': 'Weekly',
        'description': 'This API endpoint returns **weekly** time series of the stock symbol data.'
                       '**Function parameter** = _TIME_SERIES_WEEKLY_'
    },
    {
        'name': 'Monthly',
        'description': 'This API endpoint returns **monthly** time series of the stock symbol data.'
                       '**Function parameter** = _TIME_SERIES_MONTHLY_'
    }

]

# Define application
app = FastAPI(
    title='FreePI - stock market',
    description='**Fetch data from the yahoo finance and calculate crucial technical indicators.**',
    version=0.1,
    openapi_tags=tags_metadata
)


def create_response(func):
    @wraps(func)
    async def wrapper(request: Request, *args, **kwargs) -> Dict:
        results = await func(request, *args, **kwargs)
        response = {
            'Meta Data': {
                '1. Message': results['message'],
                '2. Symbol': results['symbol'],
                '3. Method': request.method,
                '4. Status-code': results['status-code'],
                '5. Timestamp': datetime.now().isoformat(),
                '6. Url': request.url._url
            }
        }
        if 'data' in results:
            response['data'] = results['data']
        return response

    return wrapper


@app.get('/', tags=['General'])
@create_response
async def _index(request: Request) -> Dict:
    """

    :param request:
    :return:
    """
    response = {
        'message': HTTPStatus.OK.phrase,
        'symbol': None,
        'status-code': HTTPStatus.OK,
        'data': {}
    }
    return response


@app.get('/data', tags=['Daily', 'Weekly', 'Monthly'])
@create_response
async def _read_data(request: Request, symbol: str, function: str) -> Dict:
    """
        Return the data of the stock market symbol, covering 20+ years of historical data:
        - **symbol**: stock market symbol
        - **function**: determine time series
        """
    if function == 'TIME_SERIES_WEEKLY':
        stock_data = receiver.receive_data(symbol=symbol, frequency='1wk', change_index=True)
    elif function == 'TIME_SERIES_MONTHLY':
        stock_data = receiver.receive_data(symbol=symbol, frequency='1mo', change_index=True)
    elif function == 'TIME_SERIES_DAILY':
        stock_data = receiver.receive_data(symbol=symbol, frequency='1d', change_index=True)
    else:
        raise HTTPException(status_code=400, detail='Invalid function parameter')
    res = stock_data.to_json(orient='index')
    parsed = json.loads(res)
    response = {
        'message': HTTPStatus.OK.phrase,
        'symbol': symbol,
        'status-code': HTTPStatus.OK,
        'data': parsed
    }
    return response


@app.get('/indicators', tags=['MACD', 'RSI', 'EMA'])
@create_response
async def _indicators(request: Request, symbol: str, function: str, time_period: int = 14, fast_period: int = 12,
                      slow_period: int = 26, signal_period: int = 9) -> Dict:
    if function == 'MACD':
        enhanced_data = technical_indicators.get_indicator(symbol, 'MACD', fast_period=fast_period,
                                                           slow_period=slow_period, signal_period=signal_period)
    elif function == 'RSI':
        enhanced_data = technical_indicators.get_indicator(symbol, 'RSI', period=time_period)
    elif function == 'EMA':
        enhanced_data = technical_indicators.get_indicator(symbol, 'EMA', period=time_period)
    else:
        raise HTTPException(status_code=400, detail='Invalid function parameter')
    res = enhanced_data.to_json(orient='index')
    return {
        'message': HTTPStatus.OK.phrase,
        'symbol': symbol,
        'status-code': HTTPStatus.OK,
        'data': json.loads(res)
    }


# if __name__ == '__main__':
#     uvicorn.run(app, port=os.environ.get("PORT", 8000), host="127.0.0.1")
