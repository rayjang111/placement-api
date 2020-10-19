import sqlalchemy
from sqlalchemy import Table, Column, String, MetaData, Integer, Boolean, Float, DateTime
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import json
import pandas as pd
import numpy as np

db_settings = {
    'user': 'admin',
    'password': 'admin!123',
    'host': '172.168.0.29',
    'port': 5432,
    'db': 'harmony'
}


class dbUtils():

    def __init__(self):
        self.connect(db_settings)

    def connect(self, db_settings):
        '''Returns a connection and a metadata object'''
        # We connect with the help of the PostgreSQL URL
        # postgresql://federer:grandestslam@localhost:5432/tennis
        url = 'postgresql://{}:{}@{}:{}/{}'
        url = url.format(db_settings['user'], db_settings['password'], db_settings['host'], db_settings['port'],
                         db_settings['db'])

        # The return value of create_engine() is our connection object
        self.engine = sqlalchemy.create_engine(url, client_encoding='utf8')

        # We then bind the connection to MetaData()
        # meta = sqlalchemy.MetaData(bind=con, reflect=True)

        return self.engine  # , meta

    def get_consolidation_status(self, prvdType=None, datatype=None, time=None, cluster_resource_id=None,
                                 zone_resource_id=None):
        where_params = list()
        if time:
            try:
                if time == "recent":
                    where_params.append(
                        " time = ( select max(time) from analytic_{prvdType}.placement_{datatype}) ".format(
                            prvdType=prvdType, datatype=datatype))
                else:
                    where_params.append(" time = '{}' ".format(time))
            except:
                self.consolidation_status_data = "time error"
                return

        if cluster_resource_id:
            try:
                where_params.append(" cluster_resource_id='{0}'".format(cluster_resource_id))
            except:
                self.consolidation_status_data = "cluster_resource_id error"
                return

        if zone_resource_id:
            try:
                where_params.append(" zone_resource_id='{}'".format(zone_resource_id))
            except:
                self.consolidation_status_data = "zone_resource_id error"
                return

        if where_params:
            where_clause = 'where' + 'and'.join(where_params)
            self.consolidation_status_data = pd.read_sql_query(
                "select * from analytic_{prvdType}.placement_{datatype} {where_clause}".format(prvdType=prvdType,
                                                                                               datatype=datatype,
                                                                                               where_clause=where_clause),
                self.engine)

        else:
            self.consolidation_status_data = pd.read_sql_query(
                "select * from analytic_{prvdType}.placement_{datatype} ".format(prvdType=prvdType, datatype=datatype),
                self.engine)

        self.consolidation_status_data = self.consolidation_status_data.to_json(orient='records')

    def placement_consolidation_host(self, prvdType, cluster_resource_id):

        price_per_hour= pd.read_sql_query(" SELECT ammt FROM framework.tb_cost_template  \
                                          ", self.engine
                                          )
        cost_multiplier = int(price_per_hour.iloc[0,0])

        ###consolidation data 호출
        consolidation_status = pd.read_sql_query("\
        select * from analytic_{prvdType}.placement_consolidation_status \
         where time = (select max(time) from analytic_{prvdType}.placement_consolidation_status) \
          and cluster_resource_id= '{cluster_resource_id}' ".format(prvdType=prvdType,
                                                                    cluster_resource_id=cluster_resource_id),
                                                 self.engine)

        self.placement_consolidation_status = list()
        consolidation_status.sort_values(by='number_migration')
        previous_server_shutdown = min(consolidation_status['number_server_shutdown'])
        total_hosts = max(consolidation_status['total_server'])
        count = 1
        for index, row in consolidation_status.iterrows():
            data = dict()
            data['label'] = '{count}안'.format(count=count)
            count += 1
            data['vm_move_count'] = row['number_migration']
            data['recommendation_id'] = row['consolidation_id']
            data['saving_host_count'] = [total_hosts, total_hosts - row['number_server_shutdown']]
            data['safety_operation'] = [row['workload_stability'] - row['workload_stability_improved'],
                                        row['workload_stability']]
            data['total_power'] = [row['energy_consumption'] + row['energy_saved'], row['energy_consumption']]
            data['saving_cost'] = [row['energy_consumption'] * cost_multiplier + row['energy_saved'] * cost_multiplier,
                                   row['energy_consumption'] * cost_multiplier]
            self.placement_consolidation_status.append(data)

        ### host status data 호출
        host_status = pd.read_sql_query("\
                select consolidation_id from analytic_{prvdType}.placement_host_status \
                 where time = (select max(time) from analytic_{prvdType}.placement_host_status) \
                  and cluster_resource_id= '{cluster_resource_id}' ".format(prvdType=prvdType,
                                                                            cluster_resource_id=cluster_resource_id),
                                        self.engine)

        recommendation_ids = np.unique(host_status['consolidation_id'])
        self.placement_host_status = list()
        for recommendation_id in recommendation_ids:
            host_status = pd.read_sql_query("\
                    select * from analytic_{prvdType}.placement_host_status \
                     where time = (select max(time) from analytic_{prvdType}.placement_host_status) \
                      and cluster_resource_id= '{cluster_resource_id}' \
                        and consolidation_id='{recommendation_id}'  ".format(prvdType=prvdType,
                                                                             cluster_resource_id=cluster_resource_id,
                                                                             recommendation_id=recommendation_id),
                                            self.engine)
            data = dict()
            data['recommendation_id'] = recommendation_id
            data['before'] = list()
            data['after'] = list()
            for index, row in host_status.iterrows():
                before_data = dict()
                before_data['label'] = row['host_name']
                before_data['value'] = row['previous_health_score']
                after_data = dict()
                after_data['label'] = row['host_name']
                after_data['value'] = row['host_health_score']
                data['before'].append(before_data)
                data['after'].append(after_data)
            self.placement_host_status.append(data)

        host_status = pd.read_sql_query("\
        select consolidation_id from analytic_{prvdType}.placement_host_status \
         where time = (select max(time) from analytic_{prvdType}.placement_host_status) \
          and cluster_resource_id= '{cluster_resource_id}' ".format(prvdType=prvdType,
                                                                    cluster_resource_id=cluster_resource_id),
                                        self.engine)

        recommendation_ids = np.unique(host_status['consolidation_id'])
        self.placement_host_status = list()
        for recommendation_id in recommendation_ids:
            host_status = pd.read_sql_query("\
            select * from analytic_{prvdType}.placement_host_status \
             where time = (select max(time) from analytic_{prvdType}.placement_host_status) \
              and cluster_resource_id= '{cluster_resource_id}' \
                and consolidation_id='{recommendation_id}'  ".format(prvdType=prvdType,
                                                                     cluster_resource_id=cluster_resource_id,
                                                                     recommendation_id=recommendation_id), self.engine)
            data = dict()
            data['recommendation_id'] = recommendation_id
            data['before'] = list()
            data['after'] = list()
            for index, row in host_status.iterrows():
                before_data = dict()
                before_data['label'] = row['host_name']
                before_data['value'] = row['previous_health_score']
                after_data = dict()
                after_data['label'] = row['host_name']
                after_data['value'] = row['host_health_score']
                data['before'].append(before_data)
                data['after'].append(after_data)
            self.placement_host_status.append(data)
        self.placement_consolidation_host_status = dict()
        self.placement_consolidation_host_status['recommendation_list'] = self.placement_consolidation_status
        self.placement_consolidation_host_status['packed_bubble_chart'] = self.placement_host_status
        self.placement_consolidation_host_status = json.dumps(self.placement_consolidation_host_status, ensure_ascii=False)

        return self.placement_consolidation_host_status

        # self.placement_consolidation_status = json.dumps(self.placement_consolidation_status, ensure_ascii=False)
        # return self.placement_consolidation_status

    # def placement_host(self, prvdType, cluster_resource_id):
    #
    #     host_status = pd.read_sql_query("\
    #     select consolidation_id from analytic_{prvdType}.placement_host_status \
    #      where time = (select max(time) from analytic_{prvdType}.placement_host_status) \
    #       and cluster_resource_id= '{cluster_resource_id}' ".format(prvdType=prvdType,
    #                                                                 cluster_resource_id=cluster_resource_id),
    #                                     self.engine)
    #
    #     recommendation_ids = np.unique(host_status['consolidation_id'])
    #     self.placement_host_status = list()
    #     for recommendation_id in recommendation_ids:
    #         host_status = pd.read_sql_query("\
    #         select * from analytic_{prvdType}.placement_host_status \
    #          where time = (select max(time) from analytic_{prvdType}.placement_host_status) \
    #           and cluster_resource_id= '{cluster_resource_id}' \
    #             and consolidation_id='{recommendation_id}'  ".format(prvdType=prvdType,
    #                                                                  cluster_resource_id=cluster_resource_id,
    #                                                                  recommendation_id=recommendation_id), self.engine)
    #         data = dict()
    #         data['recommendation_id'] = recommendation_id
    #         data['before'] = list()
    #         data['after'] = list()
    #         for index, row in host_status.iterrows():
    #             before_data = dict()
    #             before_data['label'] = row['host_name']
    #             before_data['value'] = row['previous_health_score']
    #             after_data = dict()
    #             after_data['label'] = row['host_name']
    #             after_data['value'] = row['host_health_score']
    #             data['before'].append(before_data)
    #             data['after'].append(after_data)
    #         self.placement_host_status.append(data)
    #     self.placement_host_status = json.dumps(self.placement_host_status, ensure_ascii=False)
    #     return self.placement_host_status

    def placement_migration(self, prvdType, cluster_resource_id, recommendation_id):

        migration_status = pd.read_sql_query("\
                         select * from analytic_{prvdType}.placement_migrations \
                          where time = (select max(time) from analytic_{prvdType}.placement_migrations) \
                           and cluster_resource_id= '{cluster_resource_id}' \
                             and consolidation_id='{recommendation_id}'  ".format(prvdType=prvdType,
                                                                                  cluster_resource_id=cluster_resource_id,
                                                                                  recommendation_id=recommendation_id),
                                             self.engine)
        self.placement_migration_status = list()
        count = 1
        for index, row in migration_status.iterrows():
            data = dict()
            data['order'] = count
            count += 1
            data['vm_name'] = row['vm_name']
            data['from'] = row['from_host']
            data['to'] = row['to_host']

            self.placement_migration_status.append(data)

        self.placement_migration_status = json.dumps(self.placement_migration_status,ensure_ascii=False)

        return self.placement_migration_status


# pd.read_sql_query("select * from analytic_{prvdType}.placement_consolidation_status where time = (select max(time) from analytic_{prvdType}.placement_consolidation_status)".format(prvdType='vmware'), db.engine)

db = dbUtils()
db.placement_consolidation_host('vmware', 'domain-c89')
# db.placement_migration('vmware', 'domain-c89', 'e768b660-0f74-11eb-a6d3-3e22fb024f3a')
#     if recent=="False":
#         self.host_status_data = pd.read_sql_query("select * from analytic_vmware.placement_host_status", self.engine)
#     else:
#         self.host_status_data = pd.read_sql_query("select * from analytic_vmware.placement_host_status where time = ( select max(time) from analytic_vmware.placement_host_status)", self.engine)
#
#     self.host_status_data = self.host_status_data.to_json(orient='records')
#
# def get_migrations(self, recent="True"):
#     if recent=="False":
#         self.migrations_data = pd.read_sql_query("select * from analytic_vmware.placement_migrations", self.engine)
#     else:
#         self.migrations_data = pd.read_sql_query("select * from analytic_vmware.placement_migrations where time = ( select max(time) from analytic_vmware.placement_migrations)", self.engine)
#
#     self.migrations_data = self.migrations_data.to_json(orient='records')
