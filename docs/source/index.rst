.. Cromwell Manager documentation master file, created by
   sphinx-quickstart on Fri Oct 13 18:34:20 2017.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Cromwell Manager's documentation!
============================================

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   installation
   support


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

Modules
-------

.. automodule:: cromwell_manager.cromwell

.. autoclass:: cromwell_manager.cromwell.Cromwell
   :members:

.. automodule:: cromwell_manager.workflow

.. autoclass:: cromwell_manager.workflow.WorkflowBase
   :members:

.. autoclass:: cromwell_manager.workflow.Workflow
   :members:

.. autoclass:: cromwell_manager.workflow.SubWorkflow
   :members:

.. automodule:: cromwell_manager.calledtask

.. autoclass:: cromwell_manager.calledtask.CalledTask
   :members:

.. autoclass:: cromwell_manager.calledtask.Shard
   :members:

.. automodule:: cromwell_manager.resource_utilization

.. autoclass:: cromwell_manager.resource_utilization.ResourceUtilization
   :members:

.. automodule:: cromwell_manager.io_util

.. autoclass:: cromwell_manager.io_util.GSObject
   :members:

.. autoclass:: cromwell_manager.io_util.HTTPObject
   :members:

.. autofunction:: cromwell_manager.io_util.open_gs_console

.. autofunction:: cromwell_manager.io_util.package_workflow_dependencies