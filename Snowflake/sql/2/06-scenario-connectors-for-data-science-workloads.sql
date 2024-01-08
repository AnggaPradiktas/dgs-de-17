
-- 6.0.0   SCENARIO: Connectors for Data Science Workloads
--         Your Data Science team has existing predictive models that will be
--         used to forecast delays for flights based on many factors, including
--         weather. They’ve been required, up until now, to do a bunch of work
--         to access the data, and transform and prepare it for their actual
--         work (Model Development and Evaluation). You will pair program with
--         your Data Science team to help them use their tools with Snowflake to
--         eliminate the costly and burdensome data preparation process, and
--         load into their tools directly.
--         PYTHON
--         We will help them get our historical on-time performance into Python
--         to show some basic visualizations. These visualizations help them
--         understand which airports experience heavy delay times by month. They
--         write this data back into Snowflake for other tools to access for
--         visualization.
--         JUPYTER NOTEBOOK
--         In this lab your work will be done in the provided Jupyter notebook
--         environment, rather than the Snowflake Web UI. By now your instructor
--         will likely have walked you through this, but here’s four pointers
--         labelled in the screenshot below, to get you started:
--         Jupyter notebook pointers
--         1: Opening a notebook starts what’s known as a kernel (the underlying
--         computational engine), and this is what will process the code
--         executed in the notebook document. The green dot here, to the left of
--         the name, identifies that this notebook is running and so has a
--         kernel associated with it.
--         2: This dot provides feedback on the status of the kernel. For
--         example, if idle it will appear white, and if grey then it indicates
--         the kernel is currently running.
--         3: You can execute code in the notebook by placing your cursor in the
--         code cells and hitting shift + enter, or the play button from the top
--         menu bar.
--         4: This blue line indicates the currently selected cell, and the
--         asterisk shows it is being executed. Once complete this asterisk will
--         be replaced by a number, according to this cell’s order of execution
--         within the notebook.
--         NOTE: closing the browser tab does not shut down the kernel for the
--         notebook. To do so right-click on the notebook name and select Shut
--         Down Kernel.
--         Shut down Jupyter notebook

-- 6.1.0   Lab A: Regression in Python

-- 6.1.1   Locate the notebook files archive (zip) on your local machine.

-- 6.1.2   Login to https://labs.snowflakeuniversity.com using the same username
--         you use for the Snowflake classroom account. You will use the
--         password you created when you first logged into the Snowflake
--         classroom account.

-- 6.1.3   Click on the upload icon - an up arrow (see screenshot below)
--         A file dialog will appear. Upload the notebook zip file identified
--         earlier. Note that your version number may differ from that in the
--         screenshot below as you are working with a later, enhanced release.
--         Once the upload is complete right click on the zip file in the
--         directory tree in Jupyter and select Extract Archive to unpack the
--         file content. This will create three top level directories,
--         de_connectors, de_snowpark and de_streaming. Each contains multiple
--         files and/or sub-directories.
--         Upload Files

-- 6.1.4   Open the notebook for instructions
--         Open the notebook LAB-A-regression.ipynb in the de_connectors
--         directory, and follow the instructions in that notebook.

