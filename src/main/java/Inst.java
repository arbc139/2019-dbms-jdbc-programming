public enum Inst {
  INVALID(-1), IMPORT_CSV(1), EXPORT_CSV(2), MANIPULATE_DATA(3), EXIT(4), TEST_BUILD_QUERY(5);

  private final int code;
  Inst(int code) {
    this.code = code;
  }

  public int getCode() {
    return code;
  }

  public static Inst getInst(int code) {
    for (Inst inst : Inst.values()) {
      if (inst.code == code) {
        return inst;
      }
    }
    return INVALID;
  }
}
