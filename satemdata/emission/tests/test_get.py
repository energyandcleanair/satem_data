from emission.gets import get_emissions


def test_get_emissions():

    # Get summed emissions by unit
    emissions = get_emissions(facility_id=None,
                              unit_id=None,
                              date_from="2021-01-01",
                              date_to="2021-12-31",
                              pollutant=None,
                              sum=True)

    assert len(emissions) > 0
    assert len(emissions.facility_id.unique()) > 0
    assert set(["facility_id", "pollutant", "unit_id", "lon_avg", "lat_avg"]) <= set(emissions.columns)
    assert "date" not in emissions.columns

    # Not summing
    facility_id = emissions.facility_id.iloc[0]
    emissions_facility = get_emissions(facility_id=facility_id,
                              unit_id=None,
                              date_from="2021-01-01",
                              date_to="2021-01-02",
                              pollutant=None,
                              sum=False)

    assert emissions_facility.facility_id.unique() == [facility_id]
    assert "date" in emissions_facility.columns
