public enum ManipulateInst {
  INVALID(-1), SHOW_TABLES(1), DESCRIBE_TABLE(2), SELECT(3), INSERT(4), DELETE(5), UPDATE(6),
  DROP_TABLE(7), BACK_TO_MAIN(8);

  private final int code;
  ManipulateInst(int code) {
    this.code = code;
  }

  public int getCode() {
    return code;
  }

  public static ManipulateInst getManipulateInst(int code) {
    for (ManipulateInst inst : ManipulateInst.values()) {
      if (inst.code == code) {
        return inst;
      }
    }
    return INVALID;
  }
}
