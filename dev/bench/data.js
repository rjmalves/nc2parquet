window.BENCHMARK_DATA = {
  "lastUpdate": 1771876729152,
  "repoUrl": "https://github.com/rjmalves/nc2parquet",
  "entries": {
    "nc2parquet Benchmarks": [
      {
        "commit": {
          "author": {
            "email": "rogerioalves.ee@gmail.com",
            "name": "Rogerio Alves",
            "username": "rjmalves"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "144b3f1615aca3c0dec0170206b15261759004bf",
          "message": "Merge pull request #1 from rjmalves/feat/nc2parquet-quality-upgrade\n\nfeat: nc2parquet production quality upgrade",
          "timestamp": "2026-02-23T16:40:27-03:00",
          "tree_id": "7b62f75eff768017202a30c285f3cd2727d8cdf4",
          "url": "https://github.com/rjmalves/nc2parquet/commit/144b3f1615aca3c0dec0170206b15261759004bf"
        },
        "date": 1771876728752,
        "tool": "cargo",
        "benches": [
          {
            "name": "combinations/unfiltered_4d_288_combos",
            "value": 1150177,
            "range": "± 12387",
            "unit": "ns/iter"
          },
          {
            "name": "combinations/range_lat_6_to_3_144_combos",
            "value": 1123536,
            "range": "± 13773",
            "unit": "ns/iter"
          },
          {
            "name": "combinations/range_lat_and_list_lon_16_combos",
            "value": 1100684,
            "range": "± 58973",
            "unit": "ns/iter"
          },
          {
            "name": "combinations/point2d_filter_8_combos",
            "value": 1091053,
            "range": "± 6263",
            "unit": "ns/iter"
          },
          {
            "name": "extraction/simple_xy_no_filter",
            "value": 762174,
            "range": "± 30992",
            "unit": "ns/iter"
          },
          {
            "name": "extraction/pres_temp_4d_no_filter",
            "value": 1152635,
            "range": "± 6486",
            "unit": "ns/iter"
          },
          {
            "name": "extraction/pres_temp_4d_range_filter",
            "value": 1133364,
            "range": "± 10203",
            "unit": "ns/iter"
          },
          {
            "name": "extraction/pres_temp_4d_point2d_filter",
            "value": 1088131,
            "range": "± 10273",
            "unit": "ns/iter"
          },
          {
            "name": "filters/range_latitude_30_45",
            "value": 1279,
            "range": "± 7",
            "unit": "ns/iter"
          },
          {
            "name": "filters/range_longitude_120_90",
            "value": 1364,
            "range": "± 21",
            "unit": "ns/iter"
          },
          {
            "name": "filters/list_latitude_25_35_50",
            "value": 1311,
            "range": "± 5",
            "unit": "ns/iter"
          },
          {
            "name": "filters/list_longitude_two_values",
            "value": 1358,
            "range": "± 8",
            "unit": "ns/iter"
          },
          {
            "name": "filters/point2d_two_locations",
            "value": 2553,
            "range": "± 10",
            "unit": "ns/iter"
          },
          {
            "name": "filters/point2d_tight_tolerance",
            "value": 2555,
            "range": "± 10",
            "unit": "ns/iter"
          },
          {
            "name": "postprocess/unit_convert/1000",
            "value": 15162,
            "range": "± 386",
            "unit": "ns/iter"
          },
          {
            "name": "postprocess/unit_convert/10000",
            "value": 19612,
            "range": "± 822",
            "unit": "ns/iter"
          },
          {
            "name": "postprocess/unit_convert/100000",
            "value": 51368,
            "range": "± 10490",
            "unit": "ns/iter"
          },
          {
            "name": "postprocess/column_rename/1000",
            "value": 750,
            "range": "± 18",
            "unit": "ns/iter"
          },
          {
            "name": "postprocess/column_rename/10000",
            "value": 780,
            "range": "± 42",
            "unit": "ns/iter"
          },
          {
            "name": "postprocess/column_rename/100000",
            "value": 952,
            "range": "± 528",
            "unit": "ns/iter"
          },
          {
            "name": "postprocess/datetime_convert/1000",
            "value": 30446,
            "range": "± 349",
            "unit": "ns/iter"
          },
          {
            "name": "postprocess/datetime_convert/10000",
            "value": 63333,
            "range": "± 593",
            "unit": "ns/iter"
          },
          {
            "name": "postprocess/datetime_convert/100000",
            "value": 385297,
            "range": "± 10982",
            "unit": "ns/iter"
          },
          {
            "name": "postprocess/formula_arithmetic/1000",
            "value": 17306,
            "range": "± 367",
            "unit": "ns/iter"
          },
          {
            "name": "postprocess/formula_arithmetic/10000",
            "value": 22602,
            "range": "± 15493",
            "unit": "ns/iter"
          },
          {
            "name": "postprocess/formula_arithmetic/100000",
            "value": 169971,
            "range": "± 96614",
            "unit": "ns/iter"
          },
          {
            "name": "postprocess/aggregate_groupby/1000",
            "value": 251289,
            "range": "± 4770",
            "unit": "ns/iter"
          },
          {
            "name": "postprocess/aggregate_groupby/10000",
            "value": 330373,
            "range": "± 7500",
            "unit": "ns/iter"
          },
          {
            "name": "postprocess/aggregate_groupby/100000",
            "value": 1067847,
            "range": "± 25411",
            "unit": "ns/iter"
          },
          {
            "name": "postprocess/pipeline_multi_step/1000",
            "value": 17196,
            "range": "± 341",
            "unit": "ns/iter"
          },
          {
            "name": "postprocess/pipeline_multi_step/10000",
            "value": 21250,
            "range": "± 588",
            "unit": "ns/iter"
          },
          {
            "name": "postprocess/pipeline_multi_step/100000",
            "value": 63568,
            "range": "± 6312",
            "unit": "ns/iter"
          },
          {
            "name": "postprocess/pipeline_batched_lazy/1000",
            "value": 83879,
            "range": "± 2001",
            "unit": "ns/iter"
          },
          {
            "name": "postprocess/pipeline_batched_lazy/10000",
            "value": 84764,
            "range": "± 32245",
            "unit": "ns/iter"
          },
          {
            "name": "postprocess/pipeline_batched_lazy/100000",
            "value": 163670,
            "range": "± 33134",
            "unit": "ns/iter"
          }
        ]
      }
    ]
  }
}