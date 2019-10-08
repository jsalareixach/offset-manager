import utils.LocationUtils;

import java.util.HashMap;
import java.util.Map;

public class FileReaderThread implements Runnable {

  /**
   * Location object
   * Contains current pointer in file
   */
  public class Location {
    // TODO 1 Make field final
    private final Integer pointer;

    public Location(Integer pointer) {
      this.pointer = pointer;
    }

    // TODO 3 provide additional constructors
    public Location(Location location) {
      this.pointer = location.pointer;
    }

    public Integer getPointer() {
      return pointer;
    }

    // TODO 2 remove setter
    /*
    public void setPointer(Integer pointer) {
      this.pointer = pointer;
    }
    */
  }

  /**
   * Location Manager object
   * Contains map of Locations for each thread
   */
  public class LocationtManager {
    // TODO 5 Make map private
    // offsets map for the runners
    private volatile Map<Integer, Location> locationMap;

    public LocationtManager() {
      this.locationMap = new HashMap<>();
    }

    // TODO 6 Getter should return a new object
    public Location getLocation(Integer runnerId) {
      return new Location(locationMap.get(runnerId));
    }

    // TODO 7 provide a new method to add copies of the objects
    public void putLocation(Integer runnerId, Location location) {
      locationMap.put(runnerId, new Location(location));
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
    Location newLocation = produce(location);

    // if the returned offset is newer than the old one, update offset map
    Location oldOffset = locationManager.getLocation(runnerId);
    if (newLocation.getPointer() > oldOffset.getPointer()) {
      locationManager.putLocation(runnerId, newLocation);
    }
  }

  /*
   * Reads from Amazon S3 source and returns the new offset
   */
  private Location produce(Location location) {
    locationUtils.someCode();

    // if condition1 is met offset is set to the new offset
    if (locationUtils.isHalfFile()) {
      // TODO 4 return new object
      location = new Location(5);
    }
    locationUtils.someCode();

    // if file is finished, offset is set to Integer.MAX_VALUE
    if (locationUtils.fileFinished()) {
      // TODO 4 return new object
      location = new Location(Integer.MAX_VALUE);
    }
    locationUtils.someCode();
    return location;
  }
}