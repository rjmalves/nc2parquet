window.BENCHMARK_DATA = {
  "lastUpdate": 1772115127432,
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
      },
      {
        "commit": {
          "author": {
            "email": "rogerioalves.ee@gmail.com",
            "name": "Rogerio Alves",
            "username": "rjmalves"
          },
          "committer": {
            "email": "rogerioalves.ee@gmail.com",
            "name": "Rogerio Alves",
            "username": "rjmalves"
          },
          "distinct": true,
          "id": "cee2681de7a7f520de4a28f91be36096255d8a8c",
          "message": "fix: update cargo-dist to v0.31.0 and audit action to v1.2.7\n\ncargo-dist v0.30.0 used deprecated macos-13 runners (removed Dec 2025).\nv0.31.0 uses macos-14/macos-15-intel. Also update audit action to fix\nCVSS 4.0 parse errors in the advisory database.\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-26T10:44:28-03:00",
          "tree_id": "b66557d07fed68eead5bf1f48a4138bef7e02f6b",
          "url": "https://github.com/rjmalves/nc2parquet/commit/cee2681de7a7f520de4a28f91be36096255d8a8c"
        },
        "date": 1772114590665,
        "tool": "cargo",
        "benches": [
          {
            "name": "combinations/unfiltered_4d_288_combos",
            "value": 1346883,
            "range": "± 17866",
            "unit": "ns/iter"
          },
          {
            "name": "combinations/range_lat_6_to_3_144_combos",
            "value": 1311463,
            "range": "± 16430",
            "unit": "ns/iter"
          },
          {
            "name": "combinations/range_lat_and_list_lon_16_combos",
            "value": 1291870,
            "range": "± 19145",
            "unit": "ns/iter"
          },
          {
            "name": "combinations/point2d_filter_8_combos",
            "value": 1279510,
            "range": "± 14717",
            "unit": "ns/iter"
          },
          {
            "name": "extraction/simple_xy_no_filter",
            "value": 887284,
            "range": "± 29110",
            "unit": "ns/iter"
          },
          {
            "name": "extraction/pres_temp_4d_no_filter",
            "value": 1354434,
            "range": "± 15445",
            "unit": "ns/iter"
          },
          {
            "name": "extraction/pres_temp_4d_range_filter",
            "value": 1327914,
            "range": "± 7112",
            "unit": "ns/iter"
          },
          {
            "name": "extraction/pres_temp_4d_point2d_filter",
            "value": 1284430,
            "range": "± 11353",
            "unit": "ns/iter"
          },
          {
            "name": "filters/range_latitude_30_45",
            "value": 1257,
            "range": "± 31",
            "unit": "ns/iter"
          },
          {
            "name": "filters/range_longitude_120_90",
            "value": 1377,
            "range": "± 5",
            "unit": "ns/iter"
          },
          {
            "name": "filters/list_latitude_25_35_50",
            "value": 1294,
            "range": "± 4",
            "unit": "ns/iter"
          },
          {
            "name": "filters/list_longitude_two_values",
            "value": 1357,
            "range": "± 10",
            "unit": "ns/iter"
          },
          {
            "name": "filters/point2d_two_locations",
            "value": 2545,
            "range": "± 14",
            "unit": "ns/iter"
          },
          {
            "name": "filters/point2d_tight_tolerance",
            "value": 2554,
            "range": "± 13",
            "unit": "ns/iter"
          },
          {
            "name": "postprocess/unit_convert/1000",
            "value": 14550,
            "range": "± 298",
            "unit": "ns/iter"
          },
          {
            "name": "postprocess/unit_convert/10000",
            "value": 18534,
            "range": "± 462",
            "unit": "ns/iter"
          },
          {
            "name": "postprocess/unit_convert/100000",
            "value": 57677,
            "range": "± 7213",
            "unit": "ns/iter"
          },
          {
            "name": "postprocess/column_rename/1000",
            "value": 743,
            "range": "± 49",
            "unit": "ns/iter"
          },
          {
            "name": "postprocess/column_rename/10000",
            "value": 749,
            "range": "± 70",
            "unit": "ns/iter"
          },
          {
            "name": "postprocess/column_rename/100000",
            "value": 958,
            "range": "± 373",
            "unit": "ns/iter"
          },
          {
            "name": "postprocess/datetime_convert/1000",
            "value": 29376,
            "range": "± 427",
            "unit": "ns/iter"
          },
          {
            "name": "postprocess/datetime_convert/10000",
            "value": 62528,
            "range": "± 763",
            "unit": "ns/iter"
          },
          {
            "name": "postprocess/datetime_convert/100000",
            "value": 374616,
            "range": "± 14696",
            "unit": "ns/iter"
          },
          {
            "name": "postprocess/formula_arithmetic/1000",
            "value": 17051,
            "range": "± 466",
            "unit": "ns/iter"
          },
          {
            "name": "postprocess/formula_arithmetic/10000",
            "value": 22080,
            "range": "± 17791",
            "unit": "ns/iter"
          },
          {
            "name": "postprocess/formula_arithmetic/100000",
            "value": 198444,
            "range": "± 104337",
            "unit": "ns/iter"
          },
          {
            "name": "postprocess/aggregate_groupby/1000",
            "value": 254644,
            "range": "± 4035",
            "unit": "ns/iter"
          },
          {
            "name": "postprocess/aggregate_groupby/10000",
            "value": 329594,
            "range": "± 6393",
            "unit": "ns/iter"
          },
          {
            "name": "postprocess/aggregate_groupby/100000",
            "value": 1086157,
            "range": "± 29320",
            "unit": "ns/iter"
          },
          {
            "name": "postprocess/pipeline_multi_step/1000",
            "value": 16711,
            "range": "± 14674",
            "unit": "ns/iter"
          },
          {
            "name": "postprocess/pipeline_multi_step/10000",
            "value": 21394,
            "range": "± 834",
            "unit": "ns/iter"
          },
          {
            "name": "postprocess/pipeline_multi_step/100000",
            "value": 53559,
            "range": "± 9589",
            "unit": "ns/iter"
          },
          {
            "name": "postprocess/pipeline_batched_lazy/1000",
            "value": 88764,
            "range": "± 2358",
            "unit": "ns/iter"
          },
          {
            "name": "postprocess/pipeline_batched_lazy/10000",
            "value": 94006,
            "range": "± 5503",
            "unit": "ns/iter"
          },
          {
            "name": "postprocess/pipeline_batched_lazy/100000",
            "value": 166373,
            "range": "± 49516",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "rogerioalves.ee@gmail.com",
            "name": "Rogerio Alves",
            "username": "rjmalves"
          },
          "committer": {
            "email": "rogerioalves.ee@gmail.com",
            "name": "Rogerio Alves",
            "username": "rjmalves"
          },
          "distinct": true,
          "id": "83d4a11968191034905800a2f77c71b03509acd6",
          "message": "fix: replace flaky relative timing assertion with absolute ceiling\n\nOn CI runners, resource contention causes wild variance in timing\nmeasurements. A relative comparison (50x base) still fails when\nbase is artificially fast. Use a 30s absolute ceiling instead.\n\nCo-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>",
          "timestamp": "2026-02-26T11:03:07-03:00",
          "tree_id": "9da2854898dcdb5afa5fc001134a98af7e052e60",
          "url": "https://github.com/rjmalves/nc2parquet/commit/83d4a11968191034905800a2f77c71b03509acd6"
        },
        "date": 1772115126460,
        "tool": "cargo",
        "benches": [
          {
            "name": "combinations/unfiltered_4d_288_combos",
            "value": 1343072,
            "range": "± 99139",
            "unit": "ns/iter"
          },
          {
            "name": "combinations/range_lat_6_to_3_144_combos",
            "value": 1321282,
            "range": "± 21141",
            "unit": "ns/iter"
          },
          {
            "name": "combinations/range_lat_and_list_lon_16_combos",
            "value": 1292998,
            "range": "± 17001",
            "unit": "ns/iter"
          },
          {
            "name": "combinations/point2d_filter_8_combos",
            "value": 1284544,
            "range": "± 11582",
            "unit": "ns/iter"
          },
          {
            "name": "extraction/simple_xy_no_filter",
            "value": 884280,
            "range": "± 44390",
            "unit": "ns/iter"
          },
          {
            "name": "extraction/pres_temp_4d_no_filter",
            "value": 1342030,
            "range": "± 21155",
            "unit": "ns/iter"
          },
          {
            "name": "extraction/pres_temp_4d_range_filter",
            "value": 1317120,
            "range": "± 12784",
            "unit": "ns/iter"
          },
          {
            "name": "extraction/pres_temp_4d_point2d_filter",
            "value": 1274071,
            "range": "± 11247",
            "unit": "ns/iter"
          },
          {
            "name": "filters/range_latitude_30_45",
            "value": 1270,
            "range": "± 12",
            "unit": "ns/iter"
          },
          {
            "name": "filters/range_longitude_120_90",
            "value": 1401,
            "range": "± 12",
            "unit": "ns/iter"
          },
          {
            "name": "filters/list_latitude_25_35_50",
            "value": 1281,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "filters/list_longitude_two_values",
            "value": 1333,
            "range": "± 5",
            "unit": "ns/iter"
          },
          {
            "name": "filters/point2d_two_locations",
            "value": 2534,
            "range": "± 47",
            "unit": "ns/iter"
          },
          {
            "name": "filters/point2d_tight_tolerance",
            "value": 2500,
            "range": "± 7",
            "unit": "ns/iter"
          },
          {
            "name": "postprocess/unit_convert/1000",
            "value": 15279,
            "range": "± 353",
            "unit": "ns/iter"
          },
          {
            "name": "postprocess/unit_convert/10000",
            "value": 19400,
            "range": "± 607",
            "unit": "ns/iter"
          },
          {
            "name": "postprocess/unit_convert/100000",
            "value": 46339,
            "range": "± 8272",
            "unit": "ns/iter"
          },
          {
            "name": "postprocess/column_rename/1000",
            "value": 731,
            "range": "± 23",
            "unit": "ns/iter"
          },
          {
            "name": "postprocess/column_rename/10000",
            "value": 749,
            "range": "± 45",
            "unit": "ns/iter"
          },
          {
            "name": "postprocess/column_rename/100000",
            "value": 943,
            "range": "± 240",
            "unit": "ns/iter"
          },
          {
            "name": "postprocess/datetime_convert/1000",
            "value": 30840,
            "range": "± 6629",
            "unit": "ns/iter"
          },
          {
            "name": "postprocess/datetime_convert/10000",
            "value": 63851,
            "range": "± 1072",
            "unit": "ns/iter"
          },
          {
            "name": "postprocess/datetime_convert/100000",
            "value": 381904,
            "range": "± 9728",
            "unit": "ns/iter"
          },
          {
            "name": "postprocess/formula_arithmetic/1000",
            "value": 17305,
            "range": "± 16221",
            "unit": "ns/iter"
          },
          {
            "name": "postprocess/formula_arithmetic/10000",
            "value": 22297,
            "range": "± 86145",
            "unit": "ns/iter"
          },
          {
            "name": "postprocess/formula_arithmetic/100000",
            "value": 157943,
            "range": "± 107277",
            "unit": "ns/iter"
          },
          {
            "name": "postprocess/aggregate_groupby/1000",
            "value": 247007,
            "range": "± 4154",
            "unit": "ns/iter"
          },
          {
            "name": "postprocess/aggregate_groupby/10000",
            "value": 321831,
            "range": "± 6759",
            "unit": "ns/iter"
          },
          {
            "name": "postprocess/aggregate_groupby/100000",
            "value": 1064408,
            "range": "± 20441",
            "unit": "ns/iter"
          },
          {
            "name": "postprocess/pipeline_multi_step/1000",
            "value": 17371,
            "range": "± 328",
            "unit": "ns/iter"
          },
          {
            "name": "postprocess/pipeline_multi_step/10000",
            "value": 21121,
            "range": "± 750",
            "unit": "ns/iter"
          },
          {
            "name": "postprocess/pipeline_multi_step/100000",
            "value": 56941,
            "range": "± 60006",
            "unit": "ns/iter"
          },
          {
            "name": "postprocess/pipeline_batched_lazy/1000",
            "value": 86794,
            "range": "± 1922",
            "unit": "ns/iter"
          },
          {
            "name": "postprocess/pipeline_batched_lazy/10000",
            "value": 92600,
            "range": "± 7304",
            "unit": "ns/iter"
          },
          {
            "name": "postprocess/pipeline_batched_lazy/100000",
            "value": 161878,
            "range": "± 42428",
            "unit": "ns/iter"
          }
        ]
      }
    ]
  }
}