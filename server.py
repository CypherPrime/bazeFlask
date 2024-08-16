from flask import Flask, request
from flask_restful import Resource, Api
from sqlalchemy import create_engine, Column, Integer, String
from blazegraph import Blazegraph

# Initialize Flask app and API
app = Flask(__name__)
api = Api(app)

# database connection
blazegraph_client = Blazegraph()
sqlalchemy_engine = create_engine('postgresql://user:password@host:port/database')

# Define database models
class BlazegraphData(Base):
    __tablename__ = 'blazegraph_data'
    id = Column(Integer, primary_key=True)
    data = Column(String)

class SQLData(Base):
    __tablename__ = 'sql_data'
    id = Column(Integer, primary_key=True)
    data = Column(String)

# Create database tables (if not already created)
Base.metadata.create_all(sqlalchemy_engine)

# API endpoints for Blazegraph data
class BlazegraphDataAPI(Resource):
    def get(self):
        # Query Blazegraph and return data
        return blazegraph_client.query('SELECT * WHERE { ?s ?p ?o }')

    def post(self):
        # Upload and load TTL data into Blazegraph
        data = request.files['data'].read()
        blazegraph_client.load_ttl(data)
        return {'message': 'Data uploaded successfully'}

# API endpoints for SQL data
class SQLDataAPI(Resource):
    def get(self):
        # Query SQL database and return data
        with sqlalchemy_engine.connect() as conn:
            result = conn.execute('SELECT * FROM sql_data')
            return [dict(row) for row in result]

    def post(self):
        # Insert data into SQL database
        data = request.get_json()
        with sqlalchemy_engine.connect() as conn:
            conn.execute(
                'INSERT INTO sql_data (data) VALUES (:data)',
                data=data['data']
            )
        return {'message': 'Data inserted successfully'}

# Add API resources to the API
api.add_resource(BlazegraphDataAPI, '/blazegraph/data')
api.add_resource(SQLDataAPI, '/sql/data')

# Running server
if __name__ == '__main__':
    app.run(debug=True)