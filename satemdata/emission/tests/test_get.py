from emission.gets import get_emissions


def test_get_emissions():

    # Get summed emissions by unit
    emissions = get_emissions(facility_id=None,
                              unit_id=None,
                              date_from="2021-01-01",
                              date_to="2021-01-31",
                              pollutant=None,
                              sum=True)

    assert len(emissions) > 0
    assert len(emissions.facility_id.unique()) > 0
    assert set(["facility_id", "pollutant", "unit_id", "lon", "lat"]) <= set(emissions.columns)
    assert "date" not in emissions.columns

    # Export for Stefanos to extract
    emissions[emissions.pollutant=="nox"] \
        .groupby(['facility_id','pollutant','lon','lat']) \
        .emission.sum().reset_index() \
        .sort_values(['emission'], ascending=False) \
        .head(100) \
        .to_csv("top100_facilities.csv", index_label=False)


    # Not summing
    facility_id = emissions.facility_id.iloc[0]
    emissions_facility = get_emissions(facility_id=facility_id,
                              unit_id=None,
                              date_from="2021-01-01",
                              date_to="2021-01-02",
                              pollutant=None,
                              sum=False)

    assert emissions_facility.facility_id.unique() == [facility_id]
    assert set(["facility_id", "pollutant", "unit_id", "lon", "lat"]) <= set(emissions_facility.columns)
    assert "date" in emissions_facility.columns

    # Pollutant filtering
    emissions_facility_nox = get_emissions(facility_id=facility_id,
                                           date_from="2019-01-01",
                                           date_to="2019-01-31",
                                           pollutant="nox")

    assert emissions_facility_nox.facility_id.unique() == [facility_id]
    assert emissions_facility_nox.pollutant.unique() == ['nox']
    assert set(["facility_id", "pollutant", "unit_id", "lon", "lat"]) <= set(emissions_facility.columns)
    assert "date" in emissions_facility.columns

