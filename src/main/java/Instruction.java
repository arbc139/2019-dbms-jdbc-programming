public enum Instruction{
  INVALID(-1), IMPORT_CSV(1), EXPORT_CSV(2), MANIPULATE_DATA(3), EXIT(4), TEST_BUILD_QUERY(5);

  private final int code;
  Instruction(int code) {
    this.code = code;
  }

  public int getCode() {
    return code;
  }

  public static Instruction getInstruction(int code) {
    for (Instruction inst : Instruction.values()) {
      if (inst.code == code) {
        return inst;
      }
    }
    return INVALID;
  }
}
