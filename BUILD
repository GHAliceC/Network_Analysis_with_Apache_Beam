load(
    "//devtools/python/blaze:pytype.bzl",
    "pytype_strict_binary",
    "pytype_strict_contrib_test",
    "pytype_strict_library",
)

package(
    default_applicable_licenses = ["//third_party/py/apache_beam:license"],
    default_visibility = ["//visibility:private"],
)

pytype_strict_library(
    name = "schema",
    srcs = ["schema.py"],
)

pytype_strict_library(
    name = "pipeline",
    srcs = ["pipeline.py"],
    deps = [
        ":schema",
        "//third_party/py/apache_beam",
    ],
)

pytype_strict_binary(
    name = "pipeline_main",
    srcs = ["pipeline_main.py"],
    deps = [
        ":pipeline",
        "//third_party/py/absl:app",
        "//third_party/py/absl/flags",
    ],
)

pytype_strict_contrib_test(
    name = "pipeline_test",
    srcs = ["pipeline_test.py"],
    deps = [
        ":pipeline",
        ":schema",
        "//testing/pybase",
        "//testing/pybase:parameterized",
        "//third_party/py/apache_beam",
    ],
)
