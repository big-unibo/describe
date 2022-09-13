package it.unibo.conversational.datatypes;

import com.google.common.base.Optional;
import it.unibo.conversational.Utils.DataType;

import java.io.Serializable;
import java.util.Objects;


/** A reference to an element in the DW. */
public class Entity implements Serializable {
  /** serialVersionUID */
  private static final long serialVersionUID = 9167592374910910963L;
  private Optional<String> dataTable;
  /** Table of the element. */
  private final Optional<String> metaTable;
  /** Name of the reference in the other table. */
  private final Optional<String> nameinOtherTable;
  /** Name of the element. */
  private final String nameInTable;
  /** Primary key of the element in its table. */
  private final Optional<String> pkInTable;
  /** Reference to other table. A member refers to the corresponding level. A level refers to the corresponding table. */
  private final Optional<String> refToOtherTable;
  /** Type of the entity in the database. */
  private final Optional<DataType> typeInDB;

  /**
   * Necessary for testing type checker
   * @param nameInTable name in the table
   */
  public Entity(final String pkInTable, final String nameInTable, final String refToOtherTable, final DataType typeInDB) {
    this(Optional.of(pkInTable), nameInTable, Optional.of(refToOtherTable), Optional.absent(), Optional.of(typeInDB), Optional.absent(), Optional.absent());
  }

  /**
   * @param pkInTable primary key
   * @param nameInTable name in table
   * @param refToOtherTable reference to other table
   * @param nameInOtherTable name in other table
   * @param typeInDB type in the database
   * @param metaTable name of the table in meta DB
   * @param dataTable name of the table in data DB
   */
  public Entity(//
                final String pkInTable, //
                final String nameInTable, //
                final String refToOtherTable, //
                final String nameInOtherTable, //
                final DataType typeInDB, //
                final String metaTable, //
                final String dataTable) {
    this(Optional.of(pkInTable), nameInTable, Optional.of(refToOtherTable), Optional.of(nameInOtherTable), Optional.of(typeInDB), Optional.of(metaTable), Optional.of(dataTable));
  }

  /**
   * Necessary to handle meta-entities that are not Levels or Members
   * @param pkInTable primary key
   * @param nameInTable name in table
   * @param metaTable table name
   */
  public Entity(final String pkInTable, final String nameInTable, final String metaTable) {
    this(Optional.of(pkInTable), nameInTable, Optional.absent(), Optional.absent(), Optional.absent(), Optional.of(metaTable), Optional.absent());
  }

  /**
   * @param pkInTable primary key
   * @param nameInTable name in table
   * @param refToOtherTable reference to other table
   * @param nameInOtherTable name in other table
   * @param typeInDB type in the database
   * @param metaTable name of the table in meta DB
   * @param dataTable name of the table in data DB
   */
  public Entity(
          final Optional<String> pkInTable,
          final String nameInTable,
          final Optional<String> refToOtherTable,
          final Optional<String> nameInOtherTable,
          final Optional<DataType> typeInDB,
          final Optional<String> metaTable,
          final Optional<String> dataTable) {
    this.pkInTable = pkInTable;
    this.nameInTable = nameInTable;
    this.refToOtherTable = refToOtherTable;
    this.nameinOtherTable = nameInOtherTable;
    this.typeInDB = typeInDB;
    this.metaTable = metaTable;
    this.dataTable = dataTable;
  }

  /**
   * Necessary to handle "dummy entities"
   * @param nameInTable name in the table
   */
  public Entity(final String nameInTable) {
    this(Optional.absent(), nameInTable, Optional.absent(), Optional.absent(), Optional.absent(), Optional.absent(), Optional.absent());
  }

  /**
   * Necessary to handle numeric values
   * @param nameInTable name in the table
   */
  public Entity(final String nameInTable, final DataType typeInDB) {
    this(Optional.absent(), nameInTable, Optional.absent(), Optional.absent(), Optional.of(typeInDB), Optional.absent(), Optional.absent());
  }

  public String dataTable() {
    return dataTable.get();
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj instanceof Entity) {
      final Entity o = (Entity) obj;
      return pkInTable.equals(o.pkInTable)//
              && nameInTable.equals(o.nameInTable)//
              && refToOtherTable.equals(o.refToOtherTable)//
              && nameinOtherTable.equals(o.nameinOtherTable)//
              && typeInDB.equals(o.typeInDB)//
              && metaTable.equals(o.metaTable);
    }
    return false;
  }

  public String fullQualifier() {
    return dataTable() + "." + nameInTable;
  }

  public DataType getTypeInDB() {
    return typeInDB.or(DataType.OTHER);
  }

  @Override
  public int hashCode() {
    return Objects.hash(pkInTable, nameInTable, refToOtherTable, nameinOtherTable, typeInDB, metaTable);
  }

  public String metaTable() {
    return metaTable.get();
  }

  public String nameinOtherTable() {
    return nameinOtherTable.get();
  }

  public String nameInTable() {
    return nameInTable;
  }

  public String pkInTable() {
    return pkInTable.get();
  }

  public String refToOtherTable() {
    return refToOtherTable.get();
  }

  @Override
  public String toString() {
    return nameInTable;
  }
}
