.. configuration:

Configuration
==============

The configuration of a maki nage application is done via a `yaml
<https://yaml.org/>`__ configuration file. This configuration is organized into
dedicated sections for each element of the application.

In the following documentation, each field is documented with a type. The
meaning of each type is:

* object: A yaml mapping
* list: A yaml collection
* string: A string
* module: A string describing a python module. The syntax follows the python "import" notation, i.e. modules separated by dots.
* function: A string describing a python function. The syntax is a module name followed by a function name. Both are separated by ":".

kafka (object)
------------------

This section contains the kafka configuration. It contains the following fields:

* endpoint: [string] The kafka server endpoint. e.g. "localhost"

topics (list)
------------------

This sections describes the different topics that can be used by the operators.
Each entry contains the following fields:

* name: [string] values
* encoder: [module, optional] The encoder used for kafka records. default: "makinage.encoding.string"
* partition_selector: [module, optional]. default: int(random.random() * 1000)

operators (object)
------------------

This section contains the list of operators in the application. Each operator
field is a named object of this section. Each operator object contains the
following fields:

* factory: [function] The name of the operator factory.
* sources: [list, optional] The list of source observables for this operator.
* sinks: [list, optional] The list of sink observables for this operator.

config (object)
------------------


redirect (object)
------------------

If the configuration file contains a *redirect* section the the actual content
of the configuration is retrived from the location described in this section.

It contains the following fields:

* connector: [string] The type of redirection to do. Only "consul" is supported for now.
* endpoint: [stirng] The consul endpoint url. e.g. "http://localhost:8500"
* key: [string] The name of the KV Store key to read. e.g. "myservice"
