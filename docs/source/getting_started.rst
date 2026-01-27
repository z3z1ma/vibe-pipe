Getting Started
===============

Installation
------------

Install Vibe Piper using pip:

.. code-block:: bash

   pip install vibe-piper

Or for development:

.. code-block:: bash

   git clone https://github.com/your-org/vibe-piper.git
   cd vibe-piper
   pip install -e ".[dev]"

Basic Usage
-----------

Creating a Pipeline
~~~~~~~~~~~~~~~~~~~

A Pipeline is a sequence of transformations applied to data:

.. code-block:: python

   from vibe_piper import Pipeline, Stage

   pipeline = Pipeline(
       name="my_pipeline",
       description="Processes text data"
   )

Adding Stages
~~~~~~~~~~~~~

Stages are individual transformation steps:

.. code-block:: python

   # Define a transformation function
   def remove_special_chars(text: str) -> str:
       import re
       return re.sub(r'[^a-zA-Z0-9\s]', '', text)

   # Add it as a stage
   pipeline.add_stage(
       Stage(
           name="clean_text",
           transform=remove_special_chars,
           description="Removes special characters from text"
       )
   )

   # Add more stages
   pipeline.add_stage(
       Stage(name="normalize_spaces", transform=lambda x: ' '.join(x.split()))
   )

Running a Pipeline
~~~~~~~~~~~~~~~~~~

Execute all stages in sequence:

.. code-block:: python

   result = pipeline.run("  Hello,   World!!!  ")
   # Result: "Hello World"

Next Steps
----------

* Check out the :doc:`api/index` for detailed API documentation
* See :doc:`development` for development setup and guidelines
* Read :doc:`contributing` to learn how to contribute
