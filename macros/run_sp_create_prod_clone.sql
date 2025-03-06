{% macro run_sp_create_prod_clone() %}
    {% set clone_query %}
    call maya._internal.create_prod_clone(
        'maya',
        'maya_dev',
        'internal_dev'
    );
{% endset %}
    {% do run_query(clone_query) %}
{% endmacro %}
