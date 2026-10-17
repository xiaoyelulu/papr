import { describe, expect, it } from "vitest";
import { folderAncestors, folderDescendants, folderPath, folderTree, sortFolders } from "./folders";
import type { Folder } from "../types";
const folders: Folder[] = [
  {id: 3, name: "Leaf", position: 0, parentId: 2},
  {id: 1, name: "Root", position: 1, parentId: null},
  {id: 2, name: "Child", position: 2, parentId: 1},
  {id: 4, name: "Other", position: 3, parentId: null},
];
describe("folder hierarchy", () => {
  it("builds nested directories even when children precede their parents", () => {
    const tree=folderTree(folders);
    expect(tree.map(n=>n.folder.id)).toEqual([4,1]);
    expect(tree[1].children[0].children[0].depth).toBe(2);
    expect(folderAncestors(3,folders).map(f=>f.id)).toEqual([3,2,1]);
    expect(folderPath(3,folders)).toBe("Root / Child / Leaf");
  });
  it("selects the complete subtree without unrelated folders", () => {
    expect([...folderDescendants(1,folders)]).toEqual([1,2,3]);
    expect([...folderDescendants(2,folders)]).toEqual([2,3]);
  });
  it("keeps orphans visible and bounds cyclic legacy data", () => {
    const broken=folders.map(f=>({...f,parentId:f.id===1?3:f.parentId}));
    expect(folderAncestors(3,broken)).toHaveLength(3);
    expect(folderTree(broken)).toHaveLength(2);
    expect(folderTree([{...folders[0],parentId:999}])[0].folder.id).toBe(3);
  });
});

describe("folder name order", () => {
  const folder = (id: number, name: string, parentId: number | null = null): Folder => ({ id, name, parentId, position: id });
  it("naturally sorts numeric prefixes at every level instead of saved positions", () => {
    const input = [folder(1, "10 归档"), folder(2, "2 阅读"), folder(3, "01 收集"), folder(4, "10 API", 3), folder(5, "2 API", 3), folder(6, "01 API", 3)];
    const before = input.map(f => ({ ...f }));
    const tree = folderTree(input);
    expect(tree.map(n => n.folder.name)).toEqual(["01 收集", "2 阅读", "10 归档"]);
    expect(tree[0].children.map(n => n.folder.name)).toEqual(["01 API", "2 API", "10 API"]);
    expect(input).toEqual(before);
    expect(sortFolders([...input.filter(f => f.parentId === null), folder(999, "03 新建")]).map(f => f.name)).toEqual(["01 收集", "2 阅读", "03 新建", "10 归档"]);
  });
  it("uses Chinese name collation and deterministic ties without numeric prefixes", () => {
    expect(sortFolders([folder(1, "中文"), folder(2, "编程"), folder(3, "阅读")]).map(f => f.name)).toEqual(["编程", "阅读", "中文"]);
    expect(sortFolders([folder(4, "2 AI"), folder(5, "02 AI"), folder(6, "10 AI")]).map(f => f.name)).toEqual(["02 AI", "2 AI", "10 AI"]);
  });
});
