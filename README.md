# eodash-catalog

This repository allows generation of STAC catalogs based on the configuration yaml files provided in the repository.
The configuration options are described in the [Wiki](../../wiki).

The generation of the catalog runs automatically through github actions when pushing/merging to main branch.
The generated STAC catalogs are deployed through github pages.

A preview of the catalogs can be seen using the [Stac Browser](https://radiantearth.github.io/stac-browser/#/)

Here are preloaded preview links for the catalogs available through gh-pages:
* [RACE Instance](https://radiantearth.github.io/stac-browser/#/external/eurodatacube.github.io/eodash-catalog/RACE/catalog.json)
* [Trilateral Instance](https://radiantearth.github.io/stac-browser/#/external/eurodatacube.github.io/eodash-catalog/trilateral/catalog.json)
* [GTIF Instance](https://radiantearth.github.io/stac-browser/#/external/eurodatacube.github.io/eodash-catalog/GTIF/catalog.json)

In order to run the catalog generation locally you can clone the repository and ideally setup a virtual environment or similar using the requirements.txt contained in the `generators` folder.
If you are using Visual Studio Code you can use the `Python: Create environment...` tool (when pressing Ctrl+Shift+P).
Use of python >=3.10 is recommended. 

Once venv is setup, make sure it is active and run the generator with `python generate_indicators.py`. 
Currently there is only one optional flag `-vd` that can be used to activate validation of the catalog (Still WIP).

The catalogs are saved in the `build` folder. If you want to test the generated catalog locally (either in the Stac Browser or with the eodash client) we recommend using npm [http-server](https://www.npmjs.com/package/http-server), especially to avoid possible CORS issues you can run it for example with following command:
`npx http-server -p 8000 --cors`  
when located in the build folder. 
