# 21:32
import azure.functions as func
import logging
from main_manual import run_main

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.route(route="harti1http")
def harti1http(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Harti Page 1 ETL triggered.')

    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    run_main()

    # if name:
    #     return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
    # else:
    #     return func.HttpResponse(
    #          "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
    #          status_code=200
    #     )

    
    return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
