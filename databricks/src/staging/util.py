from functools import reduce
from pyspark.sql.functions import transform, exists, struct, lit

def _validate(value_col, spec):
    """
    Returns (casted_column, is_invalid_column) for one field.

    Recurses into "array" fields that declare an element_schema (array of
    structs) -- each element is validated against that sub-schema, and the
    whole array field is flagged invalid if ANY element fails.

    NOTE: at nested levels this treats every null as "missing" regardless of
    cause. At the top level, Auto Loader's own schema-conflict rescue could in
    principle be cross-checked via _rescued_data (see the flat-schema version
    of this file) to distinguish "key absent" from "raw layer already nulled
    it out due to a type conflict" -- but _rescued_data's path format for
    fields nested inside arrays isn't a documented, stable contract, so that
    distinction is deliberately not attempted below this level.
    """
    if spec.get("type") == "array" and "element_schema" in spec:
        element_schema = spec["element_schema"]

        def _elem_casted(x):
            return struct(*[
                _validate(x[name], sub_spec)[0].alias(name)
                for name, sub_spec in element_schema.items()
            ])

        def _elem_invalid(x):
            return reduce(
                lambda a, b: a | b,
                [_validate(x[name], sub_spec)[1] for name, sub_spec in element_schema.items()],
            )

        casted_array = transform(value_col, _elem_casted)
        any_elem_invalid = exists(value_col, _elem_invalid)

        missing = value_col.isNull()  # treat an empty array as present-but-empty, not missing;
                                       # add "| (size(value_col) == 0)" here if empty should count as missing
        is_invalid = (missing & spec["mandatory"]) | (value_col.isNotNull() & any_elem_invalid)
        return casted_array, is_invalid

    # leaf field: scalar, or an array of primitives (e.g. array<string>)
    cast_val = value_col.cast(spec["type"])
    missing = value_col.isNull()
    is_invalid = (missing & spec["mandatory"]) | (value_col.isNotNull() & cast_val.isNull())
    return cast_val, is_invalid

def pad_missing_columns(df, schema):
    for c in schema.keys():
        if c not in df.columns:
            df = df.withColumn(c, lit(None).cast("string"))
    return df
