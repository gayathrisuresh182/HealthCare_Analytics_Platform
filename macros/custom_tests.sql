-- Custom macros for reusable test logic

{% macro test_accepted_range(model, column_name, min_value, max_value, inclusive=true) %}
  -- Test that a column value is within an accepted range
  SELECT *
  FROM {{ model }}
  WHERE {{ column_name }} < {{ min_value }}
     OR {{ column_name }} > {{ max_value }}
{% endmacro %}

{% macro generate_date_spine(start_date, end_date) %}
  -- Generate a date spine between two dates
  SELECT
    DATEADD(DAY, SEQ4(), '{{ start_date }}') AS date_day
  FROM TABLE(GENERATOR(ROWCOUNT => 10000))
  WHERE date_day <= '{{ end_date }}'
{% endmacro %}

{% macro calculate_percentile(column_name, percentile) %}
  -- Calculate percentile using PERCENTILE_CONT
  PERCENTILE_CONT({{ percentile }}) WITHIN GROUP (ORDER BY {{ column_name }})
{% endmacro %}

