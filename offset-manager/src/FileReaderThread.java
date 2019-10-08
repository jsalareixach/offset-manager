import utils.LocationUtils;

import java.util.HashMap;
import java.util.Map;

public class FileReaderThread implements Runnable {

  /**
   * Location object
   * Contains current pointer in file
   */
  public class Location {
    private Integer pointer;

    public Location(Integer pointer) {
      this.pointer = pointer;
    }

    public Integer getPointer() {
      return pointer;
    }
    public void setPointer(Integer pointer) {
      this.pointer = pointer;
    }
  }

  /**
   * Location Manager object
   * Contains map of Locations for each thread
   */
  public class LocationtManager {
    // offsets map for the runners
    volatile Map<Integer, Location> locationMap;

    public LocationtManager() {
      this.locationMap = new HashMap<>();
    }

    public Location getLocation(Integer runnerId) {
      return locationMap.get(runnerId);
    }
  }

  /**
   * File Reader Thread class
   * Reader logic: get initial location, read some bytes, update offset
   */
  private Integer runnerId;
  private LocationtManager locationManager;
  private LocationUtils locationUtils;

  public FileReaderThread(int runnerId, LocationtManager locationManager, LocationUtils locationUtils) {
    this.runnerId = runnerId;
    this.locationManager = locationManager;
    this.locationUtils = locationUtils;
  }

  @Override
  public void run() {
    // retrieve initial offset
    Location location = locationManager.getLocation(runnerId);

    // read from file and return new offset
    Location newOffset = produce(location);

    // if the returned offset is newer than the old one, update offset map
    Location oldOffset = locationManager.getLocation(runnerId);
    if (newOffset.getPointer() > oldOffset.getPointer()) {
      locationManager.locationMap.put(runnerId, newOffset);
    }
  }

  /*
   * Reads from Amazon S3 source and returns the new offset
   */
  private Location produce(Location offset) {
    locationUtils.someCode();

    // if condition1 is met offset is set to the new offset
    if (locationUtils.isHalfFile()) {
      offset.setPointer(5);
    }
    locationUtils.someCode();

    // if file is finished, offset is set to Integer.MAX_VALUE
    if (locationUtils.fileFinished()) {
      offset.setPointer(Integer.MAX_VALUE);
    }
    locationUtils.someCode();
    return offset;
  }
}