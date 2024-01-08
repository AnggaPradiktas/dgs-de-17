
-- 2.0.0   Key Pair Authentication
--         In this lab you will learn and practice the following:
--         - Generating public and private keys for use with Snowflake
--         - Setting public keys for a user to allow authentication without
--         password
--         - Using a private key to authenticate to Snowflake
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

-- 2.1.0   Create and Install Key Pair Authentication

-- 2.1.1   Locate the notebook files archive (zip) on your local machine.

-- 2.1.2   Login to Jupyter
--         Login to https://labs.snowflakeuniversity.com using the same username
--         you use for the Snowflake classroom account. You will use the
--         password you created when you first logged into the Snowflake
--         classroom account.

-- 2.1.3   Click on the upload icon - an up arrow (see screenshot below)
--         Jupyter lab upload icon
--         A file dialog will appear. Upload the notebook zip file identified
--         earlier.
--         Once the upload is complete right click on the zip file in the
--         directory tree in Jupyter and select Extract Archive to unpack the
--         file content. This will create a top level directory, named
--         adv_key_pair_auth. It contains the notebook file.

-- 2.1.4   Open the notebook to run through the lab
--         Open the notebook by first double clicking on the adv_key_pair_auth
--         directory and then double clicking on the adv_key_pair_auth.ipynb
--         notebook file. Then follow the instructions in that notebook.
--         - Recall you can execute code in the notebook by placing your cursor
--         in the code cells and hitting shift + enter, or the play button from
--         the top menu bar.
--         - The blue line indicates the currently selected cell, and the
--         asterisk shows it is being executed. Once complete this asterisk will
--         be replaced by a number, according to this cell’s order of execution
--         within the notebook.

