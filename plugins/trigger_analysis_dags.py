import json
from io import BytesIO
from yaml import load, SafeLoader
from jsonschema import Draft202012Validator
from flask import Blueprint, redirect, url_for, flash, send_file
from airflow.models.base import Base
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.www.views import AirflowModelView
from airflow.hooks.base import BaseHook
from sqlalchemy.orm.session import Session
from airflow.models.base import Base, StringID
from airflow.security import permissions
from flask_appbuilder.actions import action
from flask_appbuilder import expose
from flask_appbuilder.security.decorators import has_access
from airflow.plugins_manager import AirflowPlugin
from flask_appbuilder import SQLA
from airflow import settings
from sqlalchemy.orm import relationship
from airflow.models.dag import DagModel
from wtforms_sqlalchemy.fields import QuerySelectField
from flask_appbuilder.fieldwidgets import Select2Widget
from airflow.api.common.trigger_dag import trigger_dag
from airflow.utils import timezone
from airflow.exceptions import DagRunAlreadyExists
from sqlalchemy import (
  Column,
  ForeignKey,
  Integer,
  String,
  Enum,
  TEXT,
  UniqueConstraint)

class Analysis(Base, LoggingMixin):
  __tablename__ = 'analysis'
  __table_args__ = (
    UniqueConstraint('analysis_name'),
    { 'mysql_engine':'InnoDB', 'mysql_charset':'utf8' })
  id = Column(Integer(), primary_key=True, nullable=False)
  analysis_name = Column('analysis_name', String(120), nullable=True)
  analysis_yaml = Column('analysis_yaml', TEXT(), nullable=True)
  errors = Column('errors', TEXT(), nullable=True)
  status = Column('status', Enum("VALIDATED", "FAILED", "REJECTED", "TRIGGRED", "UNKNOWN", name="status_enum"), nullable=False, server_default='UNKNOWN')
  dag = relationship('DagModel')
  pipeline_id = Column(StringID(), ForeignKey('dag.dag_id', onupdate="CASCADE", ondelete="SET NULL"))

  def __repr__(self):
    return f"{self.analysis_name} : {self.status}"

  @staticmethod
  @provide_session
  def get_all_analysis(session: Session = NEW_SESSION):
    """Get all pools."""
    return session.query(Analysis).all()

  @staticmethod
  @provide_session
  def get_analysis(analysis_name: str, session: Session = NEW_SESSION):
    """
    Get the Analysis with specific analysis name from the Analysis list.

    :param analysis_name: The Analysis name of the Analysis to get.
    :param session: SQLAlchemy ORM Session
    :return: the analysis object
    """
    return session.query(Analysis).filter(Analysis.analysis_name == analysis_name).first()

  @staticmethod
  @provide_session
  def create_or_update_analysis(analysis_name: str, analysis_yaml: str, session: Session = NEW_SESSION):
    """Create a Analysis with given parameters or update it if it already exists."""
    if not analysis_name:
      return
    analysis = \
      session.\
      query(Analysis).\
      filter_by(analysis_name=analysis_name).\
      first()
    if analysis is None:
      analysis = \
        Analysis(
          analysis_name=analysis_name,
          analysis_yaml=analysis_yaml)
      session.add(analysis)
    else:
      analysis.analysis_yaml = analysis_yaml

  @staticmethod
  @provide_session
  def delete_analysis(analysis_name: str, session: Session = NEW_SESSION):
    """Delete analysis by a given name."""
    analysis = \
      session.query(Analysis).\
      filter_by(analysis_name=analysis_name).\
      first()
    if analysis is None:
      raise KeyError(f"Analysis '{analysis_name}' doesn't exist")
    session.delete(analysis)
    session.commit()
    return analysis

## set DB
db = SQLA()
db.session = settings.Session

def dagmodel_query():
  try:
    dags = \
      db.session.\
      query(DagModel).\
      filter(DagModel.is_paused==False).\
      filter(DagModel.is_active==True).\
      filter(DagModel.has_import_errors==False).\
      order_by(DagModel.dag_id.asc()).\
      all()
    return dags
  except:
    raise

TEMPLATE_DATA = """reference_genome: data/ref.fasta
samples:
  - sample_id: sampleA
    fastq1: data/fastq/sampleA_R1.fastq
  - sample_id: sampleB
    fastq1: data/fastq/sampleB_R1.fastq
"""

DAG_VALIDATION_SCHEMA = """{
  "$schema": "http://json-schema.org/draft-06/schema#",
  "id": "test",
  "title": "dag validation schema",
  "description": "Schema for validation of analysis yaml file",
  "version": "0.0.0.1",
  "type" : "object",
  "uniqueItems": true,
  "minItems": 1,
  "properties": {
    "reference_genome": {
      "type": "string",
      "pattern": "^[a-zA-Z0-9-_./]+$"
    },
    "samples": {
      "type" : "array",
      "uniqueItems": true,
      "minItems": 1,
      "items":{
        "type": "object",
        "properties": {
          "sample_id": {
            "type" : "string",
            "pattern": "^[a-zA-Z0-9-_]+$"
          },
          "fastq1": {
            "type" : "string",
            "pattern": "^[a-zA-Z0-9-_./]+$"
          }
        },
        "required": ["sample_id", "fastq1"]
      }
    }
  },
  "required": ["reference_genome", "samples"]
}"""
validation_schemas = \
  dict(
    airflow_analysis_dag=DAG_VALIDATION_SCHEMA,
    nextflow_analysis_wrapper=DAG_VALIDATION_SCHEMA)

class AnalysisModelView(AirflowModelView):
  """View to show records from Analysis table"""
  route_base = "/analysis"

  datamodel = \
    AirflowModelView.CustomSQLAInterface(Analysis)
  ## permissions
  class_permission_name = "Analysis"
  method_permission_name = {
    "add": "create",
    "list": "read",
    "edit": "edit",
    "show": "read",
    "delete": "delete",
    "action_muldelete": "delete"}
  base_permissions = [
    permissions.ACTION_CAN_CREATE,
    permissions.ACTION_CAN_READ,
    permissions.ACTION_CAN_EDIT,
    permissions.ACTION_CAN_DELETE,
    permissions.ACTION_CAN_ACCESS_MENU]
  label_columns = {
    "dag": "Dag",
    "dag.dag_id": "DAG ID",
    "analysis_name": "Analysis name",
    "analysis_yaml": "Design",
    "status": "Status",
    "errors": "Errors"}
  list_columns = ["analysis_name", "status", "dag.dag_id"]
  add_columns = ["analysis_name", "analysis_yaml", "dag"]
  edit_columns = ["analysis_name", "analysis_yaml", "dag"]
  show_columns = ["analysis_name", "analysis_yaml", "status", "dag.dag_id", "errors"]
  search_columns = ["analysis_name", "analysis_yaml"]
  base_order = ("key", "asc")
  base_order = ("id", "desc")
  add_form_extra_fields = {
    "dag": QuerySelectField(
      "DagModel",
        query_factory=dagmodel_query,
        widget=Select2Widget()),
  }
  edit_form_extra_fields = {
    "dag": QuerySelectField(
      "DagModel",
        query_factory=dagmodel_query,
        widget=Select2Widget()),
  }

  @action("get_test_data", "Get test data", "Download file?", single=True, multiple=False)
  def get_test_data(self, item):
    output = BytesIO(TEMPLATE_DATA.encode('utf-8'))
    output.seek(0)
    self.update_redirect()
    return send_file(output, download_name=f"test_analysis_data.yaml", as_attachment=True)

  @action("trigger", "Trigger", "Are you sure?", single=True, multiple=False)
  def trigger_pipeline(self, item):
    if item.status != 'VALIDATED':
      flash(f"Failed to run jobs for {item.analysis_name}", "danger")
    else:
      json_data = \
        load(
          item.analysis_yaml,
          Loader=SafeLoader)
      try:
        _ = \
          trigger_dag(
            dag_id=item.dag.dag_id,
            run_id=item.analysis_name,
            conf=json_data,
            execution_date=timezone.utcnow(),
            replace_microseconds=False)
        flash(f"Submitted jobs for {item.analysis_name}", "info")
      except DagRunAlreadyExists as e:
        flash(f"Can't re-run jobs for {item.analysis_name}", "warning")
      except Exception as e:
        flash(f"Failed to run jobs for {item.analysis_name}", "danger")
    return redirect(url_for('AnalysisModelView.list'))

  @action("validate", "Validate", "Validate analysis?", single=False, multiple=True)
  def validate_analysis(self, items):
    for item in items:
      try:
        error_list = list()
        ## check dag
        dag_id = item.dag.dag_id
        if dag_id not in validation_schemas:
          error_list.append("Missing validation schema")
        ## load yaml
        try:
          json_data = \
            load(
              item.analysis_yaml,
              Loader=SafeLoader)
        except:
          error_list.append("Failed to load yaml design")
          raise
        ## load schema
        try:
          schema = \
            json.loads(
              validation_schemas.get(dag_id))
        except:
          error_list.append("Failed to load validation schema")
          raise
        ## validate design and load errors
        schema_validator = \
          Draft202012Validator(schema)
        for error in sorted(schema_validator.iter_errors(json_data), key=str):
          error_list.append(error.message)
        if len(error_list) > 0:
          errors = '\n'.join(error_list)
          status = 'FAILED'
        else:
            errors = ''
            status = 'VALIDATED'
        try:
          db.session.query(Analysis).\
          filter(Analysis.analysis_name==item.analysis_name).\
          update({'errors': errors, 'status': status})
          db.session.flush()
          db.session.commit()
        except:
          db.session.rollback()
          raise
      except:
        raise
    return redirect(url_for('AnalysisModelView.list'))

  @expose("/delete/<pk>", methods=["GET", "POST"])
  @has_access
  def delete(self, pk):
    """Single delete."""
    return super().delete(pk)

# Will show up in Connections screen in a future version
class PluginHook(BaseHook):
  pass

# Will show up under airflow.macros.test_plugin.plugin_macro
# and in templates through {{ macros.test_plugin.plugin_macro }}
def plugin_macro():
  pass

# Creating a flask blueprint to integrate the templates and static folder
bp = Blueprint(
  "pipeline_trigger",
  __name__,
  template_folder="templates",  # registers airflow/plugins/templates as a Jinja template folder
  static_folder="static",
  static_url_path="/static/pipeline_trigger",
)

v_analysis_view = AnalysisModelView()
v_analysis_package = {
    "name": "Analysis",
    "category": "Pipeline trigger",
    "view": v_analysis_view,
}

# # Defining the plugin class
class AirflowTestPlugin(AirflowPlugin):
  name = "pipeline_trigger"
  hooks = [PluginHook]
  macros = [plugin_macro]
  flask_blueprints = [bp]
  appbuilder_views = [v_analysis_package,]
  appbuilder_menu_items = []
