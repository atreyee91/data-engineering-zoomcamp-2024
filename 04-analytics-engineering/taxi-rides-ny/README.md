NY Taxi Rides DBT Model


Command to generate models:

{% set models_to_generate = codegen.get_models(directory='core', prefix='dim_') %}
{{ codegen.generate_model_yaml(
    model_names = models_to_generate
) }}
