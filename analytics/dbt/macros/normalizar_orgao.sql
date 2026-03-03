-- Define a macro
{% macro normalizar_orgao(coluna) %}
    upper(
        trim(
            regexp_replace({{ coluna }}, '\s+', ' ', 'g')
            -- remove espaços duplos, tabs, etc
        )
    )
{% endmacro %}