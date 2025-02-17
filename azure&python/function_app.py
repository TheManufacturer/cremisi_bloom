import azure.functions as func
import logging
import json
import os
import requests

import external_function as exfunc

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.function_name("dataverse_read_all")
@app.route(route="dataverse_read_all", methods=["GET"])
def dataverse_read_all(req: func.HttpRequest) -> func.HttpResponse:
    try:
        result = exfunc.dataverse_read_all()
        return func.HttpResponse(
            result, status_code=200
        )
    except Exception as e:
        error_message = f"Error in dataverse_read_all: {str(e)}"
        logging.error(error_message)
        return func.HttpResponse(
            error_message, 
            status_code=500
        )
    
@app.function_name("post_force_dataverse")
@app.route(route="post_force_dataverse", methods=["POST"])
def post_force_dataverse(req: func.HttpRequest) -> func.HttpResponse:
    try:
        result = exfunc.post_force_dataverse()
        return func.HttpResponse(
            result, status_code=200
        )
    except Exception as e:
        error_message = f"Error in post_force_dataverse: {str(e)}"
        logging.error(error_message)
        return func.HttpResponse(
            error_message, 
            status_code=500
        )
    
@app.function_name("post_active_input_dataverse")
@app.route(route="post_active_input_dataverse", methods=["POST"])
def post_active_input_dataverse(req: func.HttpRequest) -> func.HttpResponse:
    try:
        req_body = req.get_json()
        name = req_body.get("name")
        surname = req_body.get("surname")
        email = req_body.get("email")

        if not all([name, surname, email]):
            return func.HttpResponse(
                "Please provide name, surname and email", 
                status_code=400
            )
        
        result = exfunc.post_active_input_dataverse(name, surname, email)
        return func.HttpResponse(
            result, status_code=200
        )
    except Exception as e:
        error_message = f"Error in post_active_input_dataverse: {str(e)}"
        logging.error(error_message)
        return func.HttpResponse(
            error_message, 
            status_code=500
        )

@app.function_name("update_dataverse_record")
@app.route(route="update_dataverse_record", methods=["PATCH"])
def update_dataverse_record(req: func.HttpRequest) -> func.HttpResponse:
    try:
        req_body = req.get_json()
        record_id = req_body.get("record_id")
        update_data = req_body.get("update_data")

        if not all([record_id, update_data]):
            return func.HttpResponse(
                "Please provide record_id and update_data", 
                status_code=400
            )
        
        result = exfunc.update_dataverse_record(record_id, update_data)
        return func.HttpResponse(
            result, status_code=200
        )
    except Exception as e:
        error_message = f"Error in update_dataverse_record: {str(e)}"
        logging.error(error_message)
        return func.HttpResponse(
            error_message, 
            status_code=500
        )

@app.function_name("delete_dataverse_record")
@app.route(route="delete_dataverse_record", methods=["DELETE"])
def delete_dataverse_record(req: func.HttpRequest) -> func.HttpResponse:
    try:
        req_body = req.get_json()
        record_id = req_body.get("record_id")

        if not record_id:
            return func.HttpResponse(
                "Please provide record_id", 
                status_code=400
            )
        
        result = exfunc.delete_dataverse_record(record_id)
        return func.HttpResponse(
            result, status_code=200
        )
    except Exception as e:
        error_message = f"Error in delete_dataverse_record: {str(e)}"
        logging.error(error_message)
        return func.HttpResponse(
            error_message, 
            status_code=500
        )

