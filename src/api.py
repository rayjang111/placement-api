from flask import Flask, request, jsonify
from flask_restful import Resource, Api
from flask_restful import reqparse


from db_utils import dbUtils
app = Flask(__name__)
api = Api(app)

class CreateUser():

    @app.route('/core/<prvdType>/<prvdId>/<task>/<datatype>', methods=['GET'])
    def select_data(prvdType, prvdId , task, datatype):
        time = request.args.get('time')
        cluster_resource_id = request.args.get('cluster_resource_id')
        zone_resource_id = request.args.get('zone_resource_id')
        try:
            db = dbUtils()
            db.get_consolidation_status(prvdType=prvdType, datatype=datatype, time=time, cluster_resource_id=cluster_resource_id, zone_resource_id=zone_resource_id)
            return db.consolidation_status_data
        except:
            return "[]"

    @app.route('/optimization/arrangement')
    def placement_consolidation_host_status():
        prvdType = request.args.get('provider')
        path = request.args.get('path')
        cluster_resource_id=path.split('/')[3]
        zone_resource_id = path.split('/')[3]
        startdate = request.args.get('from')
        enddate = request.args.get('to')
        try:
            if prvdType=='vmware':
                db=dbUtils()
                db.placement_consolidation_host(prvdType = prvdType , cluster_resource_id= cluster_resource_id)
            if prvdType=='openstack':
                db=dbUtils()
                db.placement_consolidation_host_openstack(prvdType= prvdType, zone_resource_id= zone_resource_id)

            return db.placement_consolidation_host_status
        except:
            return "[]"

    @app.route('/optimization/arrangement/vm-migration-info')
    def placement_migrations():
        prvdType = request.args.get('provider')
        cluster_resource_id = request.args.get('cluster-id')
        zone_resource_id = request.args.get('zone-id')
        recommendation_id = request.args.get('recommendation-id')
        startdate = request.args.get('from')
        enddate = request.args.get('to')
        try:
            if prvdType == 'vmware':
                db = dbUtils()
                db.placement_migration(prvdType=prvdType, cluster_resource_id = cluster_resource_id, recommendation_id = recommendation_id)
            if prvdType == 'openstack':
                db = dbUtils()
                db.placement_migration_openstack(prvdType=prvdType, zone_resource_id = zone_resource_id, recommendation_id = recommendation_id)

            return db.placement_migration_status

        except:
            return "[]"

    @app.route('/')
    def hello():
        return 'hello'

if __name__ == '__main__':
    app.run(host='0.0.0.0',debug=True,port=5000)
