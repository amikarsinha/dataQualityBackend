from flask import Flask, request, jsonify
from sqlalchemy import create_engine, Column, Integer, String, Boolean, DateTime, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
from datetime import datetime
from flask_cors import CORS
import pandas as pd
import pyodbc
from sqlalchemy import create_engine, inspect
import urllib
import pandas as pd
import logging
import os
from dotenv import load_dotenv
load_dotenv()
app = Flask(__name__)
CORS(app)

# Database configuration
# DATABASE_URI = 'mysql+pymysql://root:admin@localhost:3306/exception_database'
# DATABASE_URI = os.getenv("DATABASE_URI")
# engine = create_engine(DATABASE_URI)


# logging config
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(name)s %(threadName)s : %(message)s')

# Azure Database configuration
DATABASE_URL = os.getenv("DATABASE_URL")

# Define the connection string
params = urllib.parse.quote_plus(DATABASE_URL)
conn_str = 'mssql+pyodbc:///?odbc_connect={}'.format(params)

# Create the azure engine
engine_azure = create_engine(conn_str, echo=True)

Base = declarative_base()

# Define a model for the table
class ExceptionRule(Base):
    __tablename__ = 'realpolicyclaimser'
    
    exception_id = Column(Integer, primary_key=True)
    table_id = Column(Integer, nullable=False)
    Severity = Column(String(50), )
    UniqueKey = Column(String(50), nullable=False)
    company = Column(String(100), nullable=False)
    created_date = Column(DateTime, nullable=False)
    department = Column(String(100), nullable=False)  
    exception_logic_system = Column(String(100), nullable=False)
    exception_name = Column(String(100), nullable=False)
    exception_owner = Column(String(100), nullable=False)
    isactive = Column(Boolean, nullable=False)
    logic = Column(String(255), nullable=False)
    pipeline_stage = Column(String(50), nullable=False)
    source_system_type = Column(String(50), nullable=False)

    def __repr__(self):
        return f'<ExceptionRule {self.exception_name}>'
    
class ExceptionResult(Base):
    __tablename__ = 'exception_result2'

    id = Column(Integer, primary_key=True, autoincrement=True)
    primary_key = Column(String(255), nullable=False)
    created_date = Column(String(255), nullable=False)
    created_time = Column(String(255), nullable=False)   # Change to Time if storing only time
    exception_id = Column(Integer, nullable=False)
    Severity = Column(String(50), )
    department = Column(String(100), nullable=False)
    isactive = Column(Boolean, nullable=False)
    logic = Column(String(255), nullable=False)



# Create the session
Session = scoped_session(sessionmaker(bind=engine_azure))
session = Session()

# Custom error handler to ensure JSON response
@app.errorhandler(Exception)
def handle_exception(e):
    response = {
        "error": str(e)
    }
    return jsonify(response), 500

@app.route('/', methods=['GET'])
def test():
    app.logger.info("Home route accessed")
    return jsonify(message="Hello, Azure!", status="success")

# Endpoint to add a new rule
@app.route('/api/rules', methods=['POST'])
def add_rule():
    try:
        # Get data from the request body
        data = request.get_json()
        app.logger.info(f'Received data: {data}')  # Log the received data
        # Create a new rule instance
        new_rule = ExceptionRule(
            table_id=data['table_id'],
            Severity=data['Severity'],
            UniqueKey=data['UniqueKey'],
            company=data['company'],
            created_date=datetime.strptime(data['created_date'], '%a, %d %b %Y %H:%M:%S %Z'),
            department=data['department'],
            exception_id=data['exception_id'],
            exception_logic_system=data['exception_logic_system'],
            exception_name=data['exception_name'],
            exception_owner=data['exception_owner'],
            isactive=bool(data['isactive']),
            logic=data['logic'],
            pipeline_stage=data['pipeline_stage'],
            source_system_type=data['source_system_type']
        )

        # Add the new rule to the session and commit to the database
        session.add(new_rule)
        session.commit()

        return jsonify({"message": "New rule added!", "rule": str(new_rule)}), 201
    
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.route('/api/rules/<int:exception_id>', methods=['PUT'])
def update_rule(exception_id):
    try:
        data = request.get_json()
        rule = session.query(ExceptionRule).filter_by(exception_id=exception_id).first()

        if not rule:
            return jsonify({"message": "Rule not found"}), 404

        rule.Severity = data.get('Severity', rule.Severity)
        rule.UniqueKey = data.get('UniqueKey', rule.UniqueKey)
        rule.company = data.get('company', rule.company)
        rule.created_date = datetime.strptime(data['created_date'], '%a, %d %b %Y %H:%M:%S %Z') if 'created_date' in data else rule.created_date
        rule.department = data.get('department', rule.department)
        rule.exception_id = data.get('exception_id', rule.exception_id)
        rule.exception_logic_system = data.get('exception_logic_system', rule.exception_logic_system)
        rule.exception_name = data.get('exception_name', rule.exception_name)
        rule.exception_owner = data.get('exception_owner', rule.exception_owner)
        rule.isactive = bool(data.get('isactive', rule.isactive))
        rule.logic = data.get('logic', rule.logic)
        rule.pipeline_stage = data.get('pipeline_stage', rule.pipeline_stage)
        rule.source_system_type = data.get('source_system_type', rule.source_system_type)

        session.commit()

        return jsonify({"message": "Rule updated!", "rule": str(rule)}), 200
    
    except Exception as e:
        session.rollback()
        return jsonify({"error": str(e)}), 400

# Endpoint to delete a rule
@app.route('/api/rules/<int:exception_id>', methods=['DELETE'])
def delete_rule(exception_id):
    try:
        rule = session.query(ExceptionRule).filter_by(exception_id=exception_id).first()

        if not rule:
            return jsonify({"message": "Rule not found"}), 404

        session.delete(rule)
        session.commit()

        return jsonify({"message": "Rule deleted!"}), 200
    
    except Exception as e:
        session.rollback()
        return jsonify({"error": str(e)}), 400

# Endpoint to retrieve all rules
@app.route('/api/rules', methods=['GET'])
def get_all_rules():
    try:
        app.logger.info("Exception record api accessed")
        rules = session.query(ExceptionRule).all()
        return jsonify([{
            "table_id": rule.table_id,
            "Severity": rule.Severity,
            "UniqueKey": rule.UniqueKey,
            "company": rule.company,
            "created_date": rule.created_date.strftime('%a, %d %b %Y %H:%M:%S %Z'),
            "department": rule.department,
            "exception_id": rule.exception_id,
            "exception_logic_system": rule.exception_logic_system,
            "exception_name": rule.exception_name,
            "exception_owner": rule.exception_owner,
            "isactive": rule.isactive,
            "logic": rule.logic,
            "pipeline_stage": rule.pipeline_stage,
            "source_system_type": rule.source_system_type
        } for rule in rules]), 200
    
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.route('/api/dataProfiling', methods=['GET'])
def data_profiling():
    try:
        app.logger.info("Data Profiling api accessed")
        table_name = 'realpolicyclaims'
        
        # Load the data into a DataFrame
        df = pd.read_sql_table(table_name, engine_azure)
        
        # Describe the dataframe
        table_data = []
        sno = 1
        for column in df.columns:
            col_name = column
            col_dtype = str(df[column].dtype)  # Convert dtype to string
            distinct_values = int(df[column].nunique())  # Convert to Python int
            null_values = int(df[column].isnull().sum())  # Convert to Python int
            table_data.append({
                "sno": sno,
                "column_name": col_name,
                "data_type": col_dtype,
                "distinct_values": distinct_values,
                "null_values": null_values
            })
            sno += 1

        return jsonify(table_data), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# get all exception recordss
@app.route('/api/executeRules', methods=['GET'])
def get_all_rules_ExecuteRules():
    try:
        rules = session.query(ExceptionRule).all()
        return jsonify([{
            "table_id": rule.table_id,
            "Severity": rule.Severity,
            "UniqueKey": rule.UniqueKey,
            "company": rule.company,
            "created_date": rule.created_date.strftime('%a, %d %b %Y %H:%M:%S %Z'),
            "department": rule.department,
            "exception_id": rule.exception_id,
            "exception_logic_system": rule.exception_logic_system,
            "exception_name": rule.exception_name,
            "exception_owner": rule.exception_owner,
            "isactive": rule.isactive,
            "logic": rule.logic,
            "pipeline_stage": rule.pipeline_stage,
            "source_system_type": rule.source_system_type
        } for rule in rules]), 200
    
    except Exception as e:
        return jsonify({"error": str(e)}), 400

# Custom error handler to ensure JSON response
@app.errorhandler(Exception)
def handle_exception(e):
    response = {
        "error": str(e)
    }
    return jsonify(response), 500

# Endpoint to execute the rules and store the results
@app.route('/api/execute_rule', methods=['POST'])
def execute_rule():
    try:
        data = request.get_json()
        # print(data)
        # print("Hello")
        # Check if the input data is a list
        if not isinstance(data, list):
            return jsonify({"error": "Invalid input format, expected a list of JSON objects"}), 400

        for item in data:
            # Extract the relevant fields
            exception_id = item.get('exception_id')
            logic = item.get('logic')
            department = item.get('department')
            Severity = item.get('Severity')
            isactive = item.get('isactive')
            if not exception_id or not logic:
                return jsonify({"error": "Both 'exception_id' and 'logic' are required in each item"}), 400

            # Execute the SQL command
            result_df = pd.read_sql_query(f'SELECT * FROM realpolicyclaims WHERE {logic}', engine_azure)

            # Iterate through the result dataframe and store each result
            for _, row in result_df.iterrows():
                primary_key = str(row['POLICY_NO'])  # Assuming 'id' is the primary key of the `realpolicyclaims` table
                created_date = str(datetime.now().date())
                created_time = str(datetime.now().time())
                print(len(created_time))
                print(primary_key + "," + created_date  + ", " + created_time + ", " + created_time)

                print("Adding exception result to db")
                # Store the result in the ExceptionResult table
                result_record = ExceptionResult(
                    primary_key=primary_key,
                    created_date=created_date,
                    created_time=created_time,  # Store formatted time string
                    exception_id=exception_id,
                    department = department,
                    Severity = Severity,
                    isactive = isactive,
                    logic=logic
                )
                
                session.add(result_record)
                print("Completed adding exception result to db")
        print("Commiting exception result to db")
        session.commit()
        print("Commited exception result to db")
        return jsonify({"message": "Logic executed and results stored successfully!"}), 200

    except Exception as e:
        session.rollback()
        return jsonify({"error": str(e)}), 400


# Get all results for exception records
@app.route('/api/exception_results', methods=['GET'])
def get_all_exception_results():
    try:
        app.logger.info("Exception Results api accessed")
        # Query all records from the exception_result table
        results = session.query(ExceptionResult).all()

        # Serialize the query results into a list of dictionaries
        result_list = [{
            "id": result.id,
            "primary_key": result.primary_key,
            "created_date": result.created_date,
            "created_time": result.created_time,
            "exception_id": result.exception_id,
            "logic": result.logic
        } for result in results]

        return jsonify(result_list), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 400
    


@app.route('/api/records-vs-id', methods=['GET'])
def create_records_vs_id_Graph():
    session = Session()
    try:
        # Use a GROUP BY query to count records for each exception_id
        exception_counts = (
            session.query(
                ExceptionResult.exception_id, 
                func.count(ExceptionResult.id).label('count')
            )
            .group_by(ExceptionResult.exception_id)
            .all()
        )
        
        # Prepare the response data
        result = [
            {'exception_id': exc_id, 'count': count}
            for exc_id, count in exception_counts
        ]
        
        # Return the data as JSON
        return jsonify(result)

    except Exception as e:
        # Print the error to the console for debugging
        print(f"Error occurred: {e}")
        session.rollback()
        # Return a JSON response indicating an internal server error
        return jsonify({'error': 'An internal server error occurred.'}), 500

    finally:
        # session.close()
        Session.remove()



@app.route('/api/records-vs-severity', methods=['GET'])
def create_records_vs_severity_Graph():
    session = Session()
    try:
        # Use a GROUP BY query to count records for each severity level
        severity_counts = (
            session.query(
                ExceptionResult.Severity, 
                func.count(ExceptionResult.id).label('count')
            )
            .group_by(ExceptionResult.Severity)
            .all()
        )
        
        # Prepare the response data
        result = [
            {'severity': severity, 'count': count}
            for severity, count in severity_counts
        ]
        
        # Return the data as JSON
        return jsonify(result)

    except Exception as e:
        # Print the error to the console for debugging
        print(f"Error occurred: {e}")
        session.rollback()
        # Return a JSON response indicating an internal server error
        return jsonify({'error': 'An internal server error occurred.'}), 500

    finally:
        Session.remove()


@app.route('/api/records-vs-department', methods=['GET'])
def create_records_vs_department_Graph():
    session = Session()
    try:
        # Use a GROUP BY query to count records for each department
        department_counts = (
            session.query(
                ExceptionResult.department, 
                func.count(ExceptionResult.id).label('count')
            )
            .group_by(ExceptionResult.department)
            .all()
        )
        
        # Prepare the response data
        result = [
            {'department': dept, 'count': count}
            for dept, count in department_counts
        ]
        
        # Return the data as JSON
        return jsonify(result)

    except Exception as e:
        # Print the error to the console for debugging
        print(f"Error occurred: {e}")
        session.rollback()
        # Return a JSON response indicating an internal server error
        return jsonify({'error': 'An internal server error occurred.'}), 500

    finally:
        Session.remove()




@app.route('/api/records-vs-state', methods=['GET'])
def create_records_vs_state_Graph():
    session = Session()
    try:
        # Use a GROUP BY query to count records for each department
        isactive_counts = (
            session.query(
                ExceptionResult.isactive, 
                func.count(ExceptionResult.id).label('count')
            )
            .group_by(ExceptionResult.isactive)
            .all()
        )
        
        # Prepare the response data
        result = [
        {'status': 'NotActive' if not isActive else 'Active', 'count': count}
        for isActive, count in isactive_counts
    ]
        
        # Return the data as JSON
        return jsonify(result)

    except Exception as e:
        # Print the error to the console for debugging
        print(f"Error occurred: {e}")
        session.rollback()
        # Return a JSON response indicating an internal server error
        return jsonify({'error': 'An internal server error occurred.'}), 500

    finally:
        Session.remove()



@app.route('/api/line-chart-data', methods=['GET'])
def get_line_chart_data():
    # Query to count active programs by month and year
    query = session.query(
        func.date_format(ExceptionResult.created_date, '%Y-%m').label('month'),
        func.count().label('active_count')
    ).filter(ExceptionResult.isactive == 0
    ).group_by(func.date_format(ExceptionResult.created_date, '%Y-%m')
    ).order_by(func.date_format(ExceptionResult.created_date, '%Y-%m')
    ).all()
    print(query)
    # Convert query result to a list of dictionaries
    result = [{'date': month, 'active_count': active_count} for month, active_count in query]

    return jsonify(result)
if __name__ == '__main__':
    # Create the database and tables if they don't exist
    Base.metadata.create_all(engine_azure)
    app.logger.info("app started...")
    # Run the Flask app on port 5000 (or any port you prefer)
    app.run(debug=True, port=5000)
