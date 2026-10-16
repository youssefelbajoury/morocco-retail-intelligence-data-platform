{% macro text_hash_sk(value) %}
    (('x' || substr(md5({{ value }}), 1, 16))::bit(64)::bigint)
{% endmacro %}
